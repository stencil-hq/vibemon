//! Admin team deletion, API keys, and rigs (`X-Admin-Token`).
//!
//! Also the driver's cluster-listener routes for `vmon admin`. Sandbox and
//! node admin routes live in `api::sandboxes` / `api::nodes`.

use axum::{
	Json, Router,
	extract::{Path, Query, State},
	http::StatusCode,
	response::IntoResponse,
	routing::{delete, get, post, put},
};
use serde::Deserialize;

use super::keys::{ApiKeyDto, KeyName, delete_team_key};
use crate::{
	admin::{self, DeletedTeam},
	auth::AdminAuth,
	error::{ApiError, ApiResult},
	state::AppState,
};

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/admin/teams/{team_id}", delete(delete_team))
		.route("/admin/teams/{team_id}/api-keys", post(create_key))
		.route("/admin/teams/{team_id}/api-keys/{key_id}", delete(delete_key))
		.route("/clusters/{cluster_id}/rigs", get(rigs_unsupported))
		.route("/clusters/{cluster_id}/rigs/{rig_id}/capacity", put(rigs_unsupported))
		.route("/clusters/{cluster_id}/rigs/{rig_id}/instances", get(rigs_unsupported))
		.route("/clusters/{cluster_id}/rigs/{rig_id}/errors", get(rigs_unsupported))
		.route("/clusters/{cluster_id}/rigs/instances/{instance_id}", delete(rigs_unsupported))
}

/// Routes `vmon admin` reaches on a driver's cluster listener, authenticated
/// by the cluster secret ([`crate::Driver::cluster_router`] adds the check).
pub fn cluster_routes() -> Router<AppState> {
	Router::new().route("/driver/v1/teams/{team}", delete(cluster_delete_team))
}

/// `?force=` of the team deletes.
#[derive(Deserialize)]
struct DeleteTeamQuery {
	#[serde(default)]
	force: bool,
}

/// `DELETE /admin/teams/{team_id}[?force=true]` ([`admin::delete_team`]).
async fn delete_team(
	State(state): State<AppState>,
	_: AdminAuth,
	Path(team_id): Path<String>,
	Query(query): Query<DeleteTeamQuery>,
) -> ApiResult<Json<DeletedTeam>> {
	let team = state
		.store
		.team_by_id(&team_id)
		.await?
		.ok_or_else(|| ApiError::not_found(format!("team '{team_id}' not found")))?;
	Ok(Json(admin::delete_team(&state, &team, query.force).await?))
}

/// `DELETE /driver/v1/teams/{team}[?force=true]`: `vmon admin delete-team`,
/// with the team by id or slug.
async fn cluster_delete_team(
	State(state): State<AppState>,
	Path(team_ref): Path<String>,
	Query(query): Query<DeleteTeamQuery>,
) -> ApiResult<Json<DeletedTeam>> {
	let team = state
		.store
		.team_by_ref(&team_ref)
		.await?
		.ok_or_else(|| ApiError::not_found(format!("team '{team_ref}' not found")))?;
	Ok(Json(admin::delete_team(&state, &team, query.force).await?))
}

/// The five `/clusters/{clusterID}/rigs…` operations. Rigs are upstream's
/// autoscaled node pools; vibemon has none, so admins get the spec's answer
/// for such deployments.
async fn rigs_unsupported(_: AdminAuth) -> ApiError {
	ApiError::not_implemented("Not implemented by this deployment")
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
