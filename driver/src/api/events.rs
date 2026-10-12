//! `GET /events/sandboxes[/{sandboxID}]`: the team's sandbox lifecycle
//! events (offset pagination).

use axum::{
	Json, Router,
	extract::{Path, RawQuery, State},
	routing::get,
};
use serde_json::Value;

use crate::{
	auth::TeamAuth,
	error::{ApiError, ApiResult},
	state::AppState,
	store::EventQuery,
};

/// Default page size of the events API.
const DEFAULT_LIMIT: u32 = 10;
/// Maximum page size.
const MAX_LIMIT: u32 = 100;

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/events/sandboxes", get(team_events))
		.route("/events/sandboxes/{sandbox_id}", get(sandbox_events))
}

/// Parses `offset`, `limit`, `orderAsc`, and repeated or comma-separated
/// `types` from the raw query string.
pub fn parse_event_query(raw: Option<&str>) -> Result<EventQuery, ApiError> {
	let mut query = EventQuery { limit: DEFAULT_LIMIT, ..EventQuery::default() };
	for (key, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
		match key.as_ref() {
			"offset" => {
				query.offset = value
					.parse()
					.map_err(|_| ApiError::bad_request("offset must be a non-negative integer"))?;
			},
			"limit" => {
				let limit: u32 = value
					.parse()
					.map_err(|_| ApiError::bad_request("limit must be an integer"))?;
				if !(1..=MAX_LIMIT).contains(&limit) {
					return Err(ApiError::bad_request(format!(
						"limit must be between 1 and {MAX_LIMIT}"
					)));
				}
				query.limit = limit;
			},
			"orderAsc" => {
				query.order_asc = value
					.parse()
					.map_err(|_| ApiError::bad_request("orderAsc must be a boolean"))?;
			},
			"types" | "types[]" => {
				query.types.extend(
					value
						.split(',')
						.map(str::trim)
						.filter(|t| !t.is_empty())
						.map(str::to_owned),
				);
			},
			_ => {},
		}
	}
	Ok(query)
}

async fn team_events(
	State(state): State<AppState>,
	auth: TeamAuth,
	RawQuery(raw): RawQuery,
) -> ApiResult<Json<Vec<Value>>> {
	let query = parse_event_query(raw.as_deref())?;
	let rows = state.store.list_events(&auth.team.id, &query).await?;
	Ok(Json(rows.iter().map(|row| row.to_api_json()).collect()))
}

async fn sandbox_events(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(sandbox_id): Path<String>,
	RawQuery(raw): RawQuery,
) -> ApiResult<Json<Vec<Value>>> {
	let mut query = parse_event_query(raw.as_deref())?;
	query.sandbox_id = Some(sandbox_id);
	let rows = state.store.list_events(&auth.team.id, &query).await?;
	Ok(Json(rows.iter().map(|row| row.to_api_json()).collect()))
}

#[cfg(test)]
mod tests {
	use super::parse_event_query;

	#[test]
	fn query_accepts_repeated_and_csv_types_and_rejects_bad_limits() {
		let q =
			parse_event_query(Some("types=a,b&types=c&limit=100&offset=3&orderAsc=true")).unwrap();
		assert_eq!(q.types, vec!["a", "b", "c"]);
		assert_eq!((q.limit, q.offset, q.order_asc), (100, 3, true));
		assert_eq!(parse_event_query(None).unwrap().limit, 10);
		assert!(parse_event_query(Some("limit=0")).is_err());
		assert!(parse_event_query(Some("limit=101")).is_err());
		assert!(parse_event_query(Some("offset=-1")).is_err());
	}
}
