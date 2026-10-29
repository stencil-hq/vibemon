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
		.merge(crate::ci::routes(&state))
		.with_state(state)
}

/// `GET /health` (unauthenticated): 204 while this driver can serve
/// linearizable store reads; 503 (500 on other store failures) otherwise.
async fn health(State(state): State<AppState>) -> Result<StatusCode, ApiError> {
	state.store.ping().await?;
	Ok(StatusCode::NO_CONTENT)
}
