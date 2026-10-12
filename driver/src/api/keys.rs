//! `/api-keys` CRUD (Bearer + `X-Team-ID`, or admin + `X-Team-ID`; never
//! `X-API-Key`).

use axum::{
	Json, Router,
	extract::{Path, State},
	http::StatusCode,
	response::IntoResponse,
	routing::{get, patch},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use crate::{
	admin,
	auth::{AuthVia, TeamAuth},
	crypto::{API_KEY_PREFIX, API_KEY_VALUE_LEN},
	error::{ApiError, ApiResult},
	state::AppState,
	store::ApiKeyRow,
};

/// Longest accepted key name.
const MAX_NAME_LEN: usize = 255;

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/api-keys", get(list_keys).post(create_key))
		.route("/api-keys/{key_id}", patch(rename_key).delete(delete_key))
}

/// E2B `IdentifierMaskingDetails`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MaskDto {
	prefix:              &'static str,
	value_length:        usize,
	masked_value_prefix: String,
	masked_value_suffix: String,
}

/// E2B `TeamUser`.
#[derive(Serialize)]
pub struct TeamUserDto {
	id:    String,
	email: Option<String>,
}

/// E2B `TeamAPIKey` / `CreatedTeamAPIKey` (with `key`).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ApiKeyDto {
	id:         String,
	#[serde(skip_serializing_if = "Option::is_none")]
	key:        Option<String>,
	name:       String,
	mask:       MaskDto,
	created_at: DateTime<Utc>,
	created_by: Option<TeamUserDto>,
	last_used:  Option<DateTime<Utc>>,
}

impl ApiKeyDto {
	/// Renders a stored key; `key` is the plaintext right after creation.
	pub fn new(row: ApiKeyRow, key: Option<String>) -> Self {
		Self {
			id: row.id,
			key,
			name: row.name,
			mask: MaskDto {
				prefix:              API_KEY_PREFIX,
				value_length:        API_KEY_VALUE_LEN,
				masked_value_prefix: row.mask_prefix,
				masked_value_suffix: row.mask_suffix,
			},
			created_at: row.created_at,
			created_by: row
				.created_by
				.map(|id| TeamUserDto { id, email: row.created_by_email }),
			last_used: row.last_used_at,
		}
	}
}

/// E2B `NewTeamAPIKey` / `UpdateTeamAPIKey`.
#[derive(Deserialize)]
pub struct KeyName {
	pub name: String,
}

impl KeyName {
	/// Trimmed, length-checked name.
	pub fn validated(&self) -> Result<&str, ApiError> {
		let name = self.name.trim();
		if name.is_empty() || name.len() > MAX_NAME_LEN {
			return Err(ApiError::bad_request(format!("name must be 1-{MAX_NAME_LEN} characters")));
		}
		Ok(name)
	}
}

async fn list_keys(
	State(state): State<AppState>,
	auth: TeamAuth,
) -> ApiResult<Json<Vec<ApiKeyDto>>> {
	auth.require(&[AuthVia::Bearer, AuthVia::Admin])?;
	let rows = state.store.list_api_keys(&auth.team.id).await?;
	Ok(Json(
		rows
			.into_iter()
			.map(|row| ApiKeyDto::new(row, None))
			.collect(),
	))
}

async fn create_key(
	State(state): State<AppState>,
	auth: TeamAuth,
	Json(body): Json<KeyName>,
) -> ApiResult<impl IntoResponse> {
	auth.require(&[AuthVia::Bearer])?;
	let name = body.validated()?;
	let (row, key) = admin::create_api_key(
		&state.store,
		&state.keys,
		&auth.team.id,
		name,
		auth.user_id.as_deref(),
	)
	.await?;
	Ok((StatusCode::CREATED, Json(ApiKeyDto::new(row, Some(key)))))
}

async fn rename_key(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(key_id): Path<String>,
	Json(body): Json<KeyName>,
) -> ApiResult<StatusCode> {
	auth.require(&[AuthVia::Bearer, AuthVia::Admin])?;
	let name = body.validated()?;
	if !state
		.store
		.rename_api_key(&auth.team.id, &key_id, name)
		.await?
	{
		return Err(ApiError::not_found(format!("API key '{key_id}' not found")));
	}
	Ok(StatusCode::OK)
}

async fn delete_key(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(key_id): Path<String>,
) -> ApiResult<StatusCode> {
	auth.require(&[AuthVia::Bearer, AuthVia::Admin])?;
	delete_team_key(&state, &auth.team.id, &key_id).await
}

/// Deletes a team key and evicts it from this driver's auth cache.
pub async fn delete_team_key(
	state: &AppState,
	team_id: &str,
	key_id: &str,
) -> ApiResult<StatusCode> {
	let hash = state
		.store
		.delete_api_key(team_id, key_id)
		.await?
		.ok_or_else(|| ApiError::not_found(format!("API key '{key_id}' not found")))?;
	state.auth_cache.invalidate_key(&hash);
	Ok(StatusCode::NO_CONTENT)
}
