//! E2B REST API routes (`spec/e2b/infra/spec/openapi.yml`).

pub mod admin;
pub mod events;
pub mod keys;
pub mod login;
pub mod nodes;
pub mod pagination;
pub mod sandboxes;
pub mod secrets;
pub mod teams;
pub mod templates;
pub mod volumes;
pub mod webhooks;

use axum::{Router, extract::State, http::StatusCode, routing::get};

use crate::{error::ApiError, state::AppState};

/// Every API route, with state applied.
pub fn router(state: AppState) -> Router {
	Router::new()
		.route("/health", get(health))
		.merge(sandboxes::routes())
		.merge(templates::routes())
		.merge(teams::routes())
		.merge(keys::routes())
		.merge(login::routes())
		.merge(secrets::routes())
		.merge(volumes::routes())
		.merge(events::routes())
		.merge(webhooks::routes())
		.merge(admin::routes())
		.merge(nodes::routes())
		.with_state(state)
}

/// `GET /health` (unauthenticated): 204 while the database answers and the
/// local KV replica is readable; 500/503 otherwise.
async fn health(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
	sqlx::query("SELECT 1").execute(state.store.pool()).await?;
	state.cluster.kv.get("health").await?;
	Ok(StatusCode::NO_CONTENT)
}
