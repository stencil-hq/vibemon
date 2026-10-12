//! Request authentication (ARCHITECTURE.md §9.2): axum
//! extractors for team-scoped, user-scoped, and admin routes.

use std::{
	collections::HashMap,
	time::{Duration, Instant},
};

use axum::{
	extract::{FromRequestParts, MatchedPath, Path},
	http::{Method, request::Parts},
};
use parking_lot::Mutex;

use crate::{
	crypto::{self, ApiKeyFormatError},
	error::ApiError,
	lifecycle::limits::normalize_sandbox_id,
	state::AppState,
	store::{Team, User, sandboxes::SandboxState},
};

/// How long a resolved API key stays cached.
const KEY_CACHE_TTL: Duration = Duration::from_secs(30);
/// Cache entries above which expired ones are swept.
const KEY_CACHE_SWEEP_AT: usize = 4096;

const HEADER_API_KEY: &str = "x-api-key";
const HEADER_TEAM_ID: &str = "x-team-id";
const HEADER_ADMIN_TOKEN: &str = "x-admin-token";
const HEADER_ACCESS_TOKEN: &str = "x-access-token";

const MSG_MISSING: &str = "authorization header is missing";
const MSG_KEY_MALFORMED: &str =
	"API key is malformed: expected the \"e2b_\" prefix, visit https://docs.e2b.dev/api-key for \
	 more information";
const MSG_KEY_INVALID: &str =
	"Invalid API key, please visit https://docs.e2b.dev/api-key for more information.";
const MSG_KEY_LOOKUP_FAILED: &str = "Cannot verify the API key, please try again";
const MSG_BAD_TOKEN: &str = "Invalid or expired access token";
const MSG_TEAM_BLOCKED: &str = "team is blocked";

/// Which credential authenticated a [`TeamAuth`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AuthVia {
	/// `X-API-Key`.
	ApiKey,
	/// `Authorization: Bearer <access token>` (+ `X-Team-ID`).
	Bearer,
	/// `X-Admin-Token` + `X-Team-ID`.
	Admin,
	/// `X-Access-Token`, restricted to this sandbox's detail and keepalive
	/// routes.
	Sandbox(String),
}

/// Team-scoped caller, the extractor of every team route.
///
/// Accepts `X-API-Key`, a user access token with `X-Team-ID` (the user's
/// default team when the header is absent), or the admin token with
/// `X-Team-ID`. A sandbox's envd token also authenticates its own detail,
/// timeout, and refresh routes, but no other team routes. Blocked teams are
/// rejected on mutating methods.
#[derive(Clone, Debug)]
pub struct TeamAuth {
	pub team:    Team,
	/// Present for bearer auth.
	pub user_id: Option<String>,
	pub via:     AuthVia,
}

impl TeamAuth {
	/// Rejects callers that authenticated with a scheme the route does not
	/// accept (e.g. `/api-keys` refuses `X-API-Key`).
	pub fn require(&self, allowed: &[AuthVia]) -> Result<(), ApiError> {
		if allowed.contains(&self.via) {
			Ok(())
		} else {
			Err(ApiError::unauthorized(MSG_MISSING))
		}
	}
}

/// A signed-in user (`Authorization: Bearer <access token>`), no team.
#[derive(Clone, Debug)]
pub struct UserAuth {
	pub user: User,
}

/// Operator access (`X-Admin-Token` equal to `--admin-token`).
#[derive(Clone, Copy, Debug)]
pub struct AdminAuth;

#[derive(Clone)]
struct CachedKey {
	team: Team,
	at:   Instant,
}

/// API-key-hash → team cache (≈30 s), held in [`crate::state::Inner`].
#[derive(Default)]
pub struct AuthCache {
	keys: Mutex<HashMap<String, CachedKey>>,
}

impl AuthCache {
	fn get(&self, hash: &str) -> Option<CachedKey> {
		self
			.keys
			.lock()
			.get(hash)
			.filter(|entry| entry.at.elapsed() < KEY_CACHE_TTL)
			.cloned()
	}

	fn put(&self, hash: String, team: Team) {
		let mut keys = self.keys.lock();
		if keys.len() >= KEY_CACHE_SWEEP_AT {
			keys.retain(|_, entry| entry.at.elapsed() < KEY_CACHE_TTL);
		}
		keys.insert(hash, CachedKey { team, at: Instant::now() });
	}

	/// Drops a key (after delete) so this driver stops accepting it at once.
	pub fn invalidate_key(&self, hash: &str) {
		self.keys.lock().remove(hash);
	}

	/// Drops every cached key of a team, so tests see a block at once. In
	/// production `vmon admin` changes teams from another process and drivers
	/// pick them up when the entries expire ([`KEY_CACHE_TTL`]).
	#[cfg(test)]
	pub fn invalidate_team(&self, team_id: &str) {
		self.keys.lock().retain(|_, entry| entry.team.id != team_id);
	}
}

fn header<'a>(parts: &'a Parts, name: &str) -> Option<&'a str> {
	parts
		.headers
		.get(name)
		.and_then(|v| v.to_str().ok())
		.map(str::trim)
		.filter(|v| !v.is_empty())
}

fn bearer(parts: &Parts) -> Option<&str> {
	let value = header(parts, "authorization")?;
	let (scheme, token) = value.split_once(' ')?;
	scheme
		.eq_ignore_ascii_case("bearer")
		.then(|| token.trim())
		.filter(|t| !t.is_empty())
}

/// Resolves an API key to its team, through the cache. On a cache fill the
/// key's `last_used_at` is stamped in the background.
pub async fn team_for_api_key(state: &AppState, key: &str) -> Result<Team, ApiError> {
	let hash = crypto::hash_api_key(key).map_err(|e| match e {
		ApiKeyFormatError::MissingPrefix => ApiError::unauthorized(MSG_KEY_MALFORMED),
		ApiKeyFormatError::InvalidFormat => {
			ApiError::unauthorized(format!("{MSG_KEY_INVALID}\nInvalid API key format"))
		},
	})?;
	if let Some(hit) = state.auth_cache.get(&hash) {
		return Ok(hit.team);
	}
	let found = state.store.team_for_api_key(&hash).await.map_err(|error| {
		tracing::error!(%error, "api key lookup failed");
		ApiError::internal(MSG_KEY_LOOKUP_FAILED)
	})?;
	let Some((key_id, team)) = found else {
		return Err(ApiError::unauthorized(format!(
			"{MSG_KEY_INVALID}\nCannot get the team for the given API key"
		)));
	};
	state.auth_cache.put(hash, team.clone());
	let store = state.store.clone();
	tokio::spawn(async move {
		if let Err(error) = store.touch_api_key(&key_id).await {
			tracing::debug!(%error, "updating api key last_used_at failed");
		}
	});
	Ok(team)
}

/// Verifies a user access token and loads the user.
pub async fn user_for_token(state: &AppState, token: &str) -> Result<User, ApiError> {
	let claims = state
		.keys
		.verify_access_token(token)
		.ok_or_else(|| ApiError::unauthorized(MSG_BAD_TOKEN))?;
	state
		.store
		.user_by_id(&claims.sub)
		.await?
		.ok_or_else(|| ApiError::unauthorized(MSG_BAD_TOKEN))
}

/// Checks `X-Admin-Token` against the configured admin token.
pub fn is_admin(state: &AppState, parts: &Parts) -> bool {
	match (state.config.admin_token.as_deref(), header(parts, HEADER_ADMIN_TOKEN)) {
		(Some(expected), Some(given)) => crypto::ct_eq(expected.as_bytes(), given.as_bytes()),
		_ => false,
	}
}

/// Only the exact sandbox detail and keepalive routes accept envd credentials.
/// Matching the router's pattern avoids treating `/sandboxes/metrics` as an ID.
fn accepts_sandbox_token(parts: &Parts) -> bool {
	let Some(path) = parts.extensions.get::<MatchedPath>() else {
		return false;
	};
	match path.as_str() {
		"/sandboxes/{sandbox_id}" => parts.method == Method::GET,
		"/sandboxes/{sandbox_id}/timeout" | "/sandboxes/{sandbox_id}/refreshes" => {
			parts.method == Method::POST
		},
		_ => false,
	}
}

async fn resolve_team(parts: &mut Parts, state: &AppState) -> Result<TeamAuth, ApiError> {
	if let Some(key) = header(parts, HEADER_API_KEY) {
		let team = team_for_api_key(state, key).await?;
		return Ok(TeamAuth { team, user_id: None, via: AuthVia::ApiKey });
	}
	if let Some(token) = bearer(parts) {
		let user = user_for_token(state, token).await?;
		let team = match header(parts, HEADER_TEAM_ID) {
			Some(team_id) => state.store.user_team(&user.id, team_id).await?,
			None => state.store.default_team(&user.id).await?,
		}
		.ok_or_else(|| {
			ApiError::unauthorized("Cannot get the team for the given user and team ID")
		})?;
		return Ok(TeamAuth { team, user_id: Some(user.id), via: AuthVia::Bearer });
	}
	if header(parts, HEADER_ADMIN_TOKEN).is_some() {
		if !is_admin(state, parts) {
			return Err(ApiError::unauthorized("Invalid admin token"));
		}
		let team_id = header(parts, HEADER_TEAM_ID).ok_or_else(|| {
			ApiError::unauthorized("X-Team-ID header is required with X-Admin-Token")
		})?;
		let team = state
			.store
			.team_by_id(team_id)
			.await?
			.ok_or_else(|| ApiError::unauthorized("Team not found"))?;
		return Ok(TeamAuth { team, user_id: None, via: AuthVia::Admin });
	}
	if accepts_sandbox_token(parts) && header(parts, HEADER_ACCESS_TOKEN).is_some() {
		let Path(raw_id) = Path::<String>::from_request_parts(parts, state)
			.await
			.map_err(|_| ApiError::unauthorized(MSG_BAD_TOKEN))?;
		let id = normalize_sandbox_id(&raw_id).map_err(|_| ApiError::unauthorized(MSG_BAD_TOKEN))?;
		let token =
			header(parts, HEADER_ACCESS_TOKEN).ok_or_else(|| ApiError::unauthorized(MSG_BAD_TOKEN))?;
		// Authenticate before looking up the sandbox: wrong tokens reveal no
		// information about whether the requested ID exists.
		if !crypto::ct_eq(state.keys.envd_token(&id).as_bytes(), token.as_bytes()) {
			return Err(ApiError::unauthorized(MSG_BAD_TOKEN));
		}
		let row = state
			.store
			.sandbox(&id)
			.await?
			.filter(|row| row.state() != SandboxState::Killed)
			.ok_or_else(|| ApiError::unauthorized(MSG_BAD_TOKEN))?;
		let team = state
			.store
			.team_by_id(&row.team_id)
			.await?
			.ok_or_else(|| ApiError::unauthorized(MSG_BAD_TOKEN))?;
		return Ok(TeamAuth { team, user_id: None, via: AuthVia::Sandbox(id) });
	}
	Err(ApiError::unauthorized(MSG_MISSING))
}

/// Blocked teams may only read.
fn check_blocked(team: &Team, parts: &Parts) -> Result<(), ApiError> {
	if team.blocked && !(parts.method.is_safe()) {
		return Err(ApiError::forbidden(MSG_TEAM_BLOCKED));
	}
	Ok(())
}

impl FromRequestParts<AppState> for TeamAuth {
	type Rejection = ApiError;

	async fn from_request_parts(
		parts: &mut Parts,
		state: &AppState,
	) -> Result<Self, Self::Rejection> {
		let auth = resolve_team(parts, state).await?;
		check_blocked(&auth.team, parts)?;
		Ok(auth)
	}
}

impl FromRequestParts<AppState> for UserAuth {
	type Rejection = ApiError;

	async fn from_request_parts(
		parts: &mut Parts,
		state: &AppState,
	) -> Result<Self, Self::Rejection> {
		let token = bearer(parts).ok_or_else(|| ApiError::unauthorized(MSG_MISSING))?;
		Ok(Self { user: user_for_token(state, token).await? })
	}
}

impl FromRequestParts<AppState> for AdminAuth {
	type Rejection = ApiError;

	#[expect(clippy::unused_async_trait_impl, reason = "signature fixed by axum's FromRequestParts")]
	async fn from_request_parts(
		parts: &mut Parts,
		state: &AppState,
	) -> Result<Self, Self::Rejection> {
		if header(parts, HEADER_ADMIN_TOKEN).is_none() {
			return Err(ApiError::unauthorized(MSG_MISSING));
		}
		if is_admin(state, parts) {
			Ok(Self)
		} else {
			Err(ApiError::unauthorized("Invalid admin token"))
		}
	}
}

#[cfg(test)]
mod tests {
	use axum::http::Request;

	use super::*;

	fn parts(headers: &[(&str, &str)]) -> Parts {
		let mut builder = Request::builder().uri("/");
		for (k, v) in headers {
			builder = builder.header(*k, *v);
		}
		builder.body(()).unwrap().into_parts().0
	}

	#[test]
	fn bearer_parsing_is_case_insensitive_and_rejects_other_schemes() {
		assert_eq!(bearer(&parts(&[("Authorization", "bearer abc")])), Some("abc"));
		assert_eq!(bearer(&parts(&[("authorization", "Bearer  abc ")])), Some("abc"));
		assert_eq!(bearer(&parts(&[("Authorization", "Basic abc")])), None);
		assert_eq!(bearer(&parts(&[("Authorization", "Bearer ")])), None);
		assert_eq!(header(&parts(&[("X-API-KEY", "e2b_1")]), HEADER_API_KEY), Some("e2b_1"));
	}

	#[test]
	fn sandbox_auth_is_rejected_by_bearer_and_admin_guards() {
		let auth = TeamAuth {
			team:    Team {
				id:         "t".into(),
				name:       "t".into(),
				slug:       "t".into(),
				limits:     crate::state::TeamLimits::default(),
				blocked:    false,
				system:     false,
				created_at: chrono::Utc::now(),
			},
			user_id: None,
			via:     AuthVia::Sandbox("iabc".into()),
		};
		for allowed in
			[&[AuthVia::Bearer][..], &[AuthVia::Admin][..], &[AuthVia::Bearer, AuthVia::Admin][..]]
		{
			assert_eq!(
				auth.require(allowed).unwrap_err().status,
				axum::http::StatusCode::UNAUTHORIZED
			);
		}
		assert!(auth.require(&[AuthVia::Sandbox("iabc".into())]).is_ok());
		assert_eq!(
			auth
				.require(&[AuthVia::Sandbox("iother".into())])
				.unwrap_err()
				.status,
			axum::http::StatusCode::UNAUTHORIZED
		);
	}

	#[test]
	fn blocked_team_may_read_but_not_mutate() {
		let team = Team {
			id:         "t".into(),
			name:       "t".into(),
			slug:       "t".into(),
			limits:     crate::state::TeamLimits::default(),
			blocked:    true,
			system:     false,
			created_at: chrono::Utc::now(),
		};
		let get = Request::get("/").body(()).unwrap().into_parts().0;
		let post = Request::post("/").body(()).unwrap().into_parts().0;
		assert!(check_blocked(&team, &get).is_ok());
		assert_eq!(
			check_blocked(&team, &post).unwrap_err().status,
			axum::http::StatusCode::FORBIDDEN
		);
	}
}
