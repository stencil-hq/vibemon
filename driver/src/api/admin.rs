//! Admin API-key management (`X-Admin-Token`). Sandbox and node admin
//! routes live in `api::sandboxes` / `api::nodes`.

use axum::{
	Json, Router,
	extract::{Path, State},
	http::StatusCode,
	response::IntoResponse,
	routing::{delete, post},
};

use super::keys::{ApiKeyDto, KeyName, delete_team_key};
use crate::{
	admin,
	auth::AdminAuth,
	error::{ApiError, ApiResult},
	state::AppState,
};

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/admin/teams/{team_id}/api-keys", post(create_key))
		.route("/admin/teams/{team_id}/api-keys/{key_id}", delete(delete_key))
}

async fn create_key(
	State(state): State<AppState>,
	_: AdminAuth,
	Path(team_id): Path<String>,
	Json(body): Json<KeyName>,
) -> ApiResult<impl IntoResponse> {
	let name = body.validated()?;
	let team = state
		.store
		.team_by_id(&team_id)
		.await?
		.ok_or_else(|| ApiError::not_found(format!("team '{team_id}' not found")))?;
	let (row, key) = admin::create_api_key(&state.store, &state.keys, &team.id, name, None).await?;
	Ok((StatusCode::CREATED, Json(ApiKeyDto::new(row, Some(key)))))
}

async fn delete_key(
	State(state): State<AppState>,
	_: AdminAuth,
	Path((team_id, key_id)): Path<(String, String)>,
) -> ApiResult<StatusCode> {
	delete_team_key(&state, &team_id, &key_id).await
}
