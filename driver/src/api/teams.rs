//! `GET /teams` (CLI login) and team metrics derived from `sandbox_events`.

use axum::{
	Json, Router,
	extract::{Path, Query, State},
	routing::get,
};
use chrono::{DateTime, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};

use crate::{
	admin,
	auth::{TeamAuth, UserAuth},
	error::{ApiError, ApiResult},
	state::AppState,
	store::events::{is_start, is_stop},
};

/// Default metrics window when `start` is omitted.
const DEFAULT_WINDOW_SECS: i64 = 3600;
/// Upper bound on returned points.
const MAX_POINTS: i64 = 300;
/// Smallest step between points.
const MIN_STEP_SECS: i64 = 5;

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/teams", get(list_teams))
		.route("/teams/{team_id}/metrics", get(team_metrics))
		.route("/teams/{team_id}/metrics/max", get(team_metrics_max))
}

/// E2B `Team`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TeamDto {
	#[serde(rename = "teamID")]
	team_id:    String,
	name:       String,
	api_key:    String,
	is_default: bool,
}

/// `GET /teams`: the user's teams with a usable API key each (the newest key
/// the user created in that team, minted on first use).
async fn list_teams(
	State(state): State<AppState>,
	UserAuth { user }: UserAuth,
) -> ApiResult<Json<Vec<TeamDto>>> {
	let mut out = Vec::new();
	for (team, is_default) in state.store.user_teams(&user.id).await? {
		let existing = state.store.latest_user_api_key(&team.id, &user.id).await?;
		let api_key =
			if let Some(key) = existing.and_then(|row| state.keys.decrypt_str(&row.key_enc).ok()) {
				key
			} else {
				let name = format!("CLI ({})", user.email);
				admin::create_api_key(&state.store, &state.keys, &team.id, &name, Some(&user.id))
					.await?
					.1
			};
		out.push(TeamDto { team_id: team.id, name: team.name, api_key, is_default });
	}
	Ok(Json(out))
}

#[derive(Deserialize)]
struct MetricsQuery {
	start:  Option<i64>,
	end:    Option<i64>,
	metric: Option<String>,
}

/// One computed sample.
#[derive(Clone, Debug, PartialEq)]
pub struct MetricPoint {
	/// Bucket start (unix seconds).
	pub ts:         i64,
	/// Peak concurrently running sandboxes within the bucket.
	pub concurrent: i64,
	/// Starts (create + resume) per second within the bucket.
	pub start_rate: f64,
}

/// E2B `TeamMetric`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct TeamMetricDto {
	timestamp:            String,
	timestamp_unix:       i64,
	concurrent_sandboxes: i64,
	sandbox_start_rate:   f64,
}

/// E2B `MaxTeamMetric`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct MaxTeamMetricDto {
	timestamp:      String,
	timestamp_unix: i64,
	value:          f64,
}

fn rfc3339(unix: i64) -> String {
	DateTime::<Utc>::from_timestamp(unix, 0)
		.unwrap_or_default()
		.to_rfc3339_opts(SecondsFormat::Secs, true)
}

/// Step so the window yields at most [`MAX_POINTS`] points.
fn step_for(range_secs: i64) -> i64 {
	((range_secs + MAX_POINTS - 1) / MAX_POINTS).max(MIN_STEP_SECS)
}

/// Buckets lifecycle events into `[start, end]` with `step`. `baseline` is
/// the running count at `start`; events must be sorted by time.
pub fn team_series(
	baseline: i64,
	events: &[(i64, bool)],
	start: i64,
	end: i64,
	step: i64,
) -> Vec<MetricPoint> {
	let mut points = Vec::new();
	let mut running = baseline.max(0);
	let mut idx = 0;
	let mut ts = start;
	while ts <= end {
		let bucket_end = ts + step;
		let mut peak = running;
		let mut starts = 0i64;
		while idx < events.len() && events[idx].0 < bucket_end {
			let (_, started) = events[idx];
			if started {
				running += 1;
				starts += 1;
				peak = peak.max(running);
			} else {
				running = (running - 1).max(0);
			}
			idx += 1;
		}
		#[allow(clippy::cast_precision_loss, reason = "per-bucket counts are small")]
		let start_rate = starts as f64 / step as f64;
		points.push(MetricPoint { ts, concurrent: peak, start_rate });
		ts = bucket_end;
	}
	points
}

async fn compute(
	state: &AppState,
	auth: &TeamAuth,
	team_id: &str,
	query: &MetricsQuery,
) -> ApiResult<Vec<MetricPoint>> {
	if auth.team.id != team_id {
		return Err(ApiError::forbidden(format!(
			"You don't have access to the metrics of team '{team_id}'"
		)));
	}
	let now = Utc::now().timestamp();
	let end = query.end.unwrap_or(now).min(now);
	let start = query.start.unwrap_or(end - DEFAULT_WINDOW_SECS);
	if start < 0 || end < 0 {
		return Err(ApiError::bad_request("start and end must be non-negative unix timestamps"));
	}
	if start > end {
		return Err(ApiError::bad_request("start must not be after end"));
	}
	let step = step_for(end - start);
	let from = DateTime::from_timestamp(start, 0).unwrap_or_default();
	let to = DateTime::from_timestamp(end + step, 0).unwrap_or_default();
	let (baseline, events) = state.store.lifecycle_timeline(team_id, from, to).await?;
	let events: Vec<(i64, bool)> = events
		.iter()
		.filter(|(_, ty)| is_start(ty) || is_stop(ty))
		.map(|(at, ty)| (at.timestamp(), is_start(ty)))
		.collect();
	Ok(team_series(baseline, &events, start, end, step))
}

/// `GET /teams/{teamID}/metrics`.
async fn team_metrics(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(team_id): Path<String>,
	Query(query): Query<MetricsQuery>,
) -> ApiResult<Json<Vec<TeamMetricDto>>> {
	let points = compute(&state, &auth, &team_id, &query).await?;
	Ok(Json(
		points
			.into_iter()
			.map(|p| TeamMetricDto {
				timestamp:            rfc3339(p.ts),
				timestamp_unix:       p.ts,
				concurrent_sandboxes: p.concurrent,
				sandbox_start_rate:   p.start_rate,
			})
			.collect(),
	))
}

/// `GET /teams/{teamID}/metrics/max?
/// metric=concurrent_sandboxes|sandbox_start_rate`.
async fn team_metrics_max(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(team_id): Path<String>,
	Query(query): Query<MetricsQuery>,
) -> ApiResult<Json<MaxTeamMetricDto>> {
	let pick: fn(&MetricPoint) -> f64 = match query.metric.as_deref() {
		#[allow(clippy::cast_precision_loss, reason = "sandbox counts are far below 2^52")]
		Some("concurrent_sandboxes") => |p| p.concurrent as f64,
		Some("sandbox_start_rate") => |p| p.start_rate,
		_ => {
			return Err(ApiError::bad_request(
				"metric must be one of: concurrent_sandboxes, sandbox_start_rate",
			));
		},
	};
	let points = compute(&state, &auth, &team_id, &query).await?;
	let best = points
		.iter()
		.fold(None::<&MetricPoint>, |best, p| match best {
			Some(b) if pick(b) >= pick(p) => Some(b),
			_ => Some(p),
		});
	let (ts, value) = best.map_or_else(|| (Utc::now().timestamp(), 0.0), |p| (p.ts, pick(p)));
	Ok(Json(MaxTeamMetricDto { timestamp: rfc3339(ts), timestamp_unix: ts, value }))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn series_tracks_peaks_rates_and_never_goes_negative() {
		// baseline 1; +1 at 3, +1 at 4, -1 at 6, -1 x3 at 12 (one extra stop).
		// A bucket's peak includes the count carried in from its start.
		let events = [(3, true), (4, true), (6, false), (12, false), (12, false), (12, false)];
		let points = team_series(1, &events, 0, 15, 5);
		assert_eq!(points.iter().map(|p| p.ts).collect::<Vec<_>>(), vec![0, 5, 10, 15]);
		assert_eq!(points.iter().map(|p| p.concurrent).collect::<Vec<_>>(), vec![3, 3, 2, 0]);
		assert!((points[0].start_rate - 0.4).abs() < f64::EPSILON);
		assert!(points[1].start_rate.abs() < f64::EPSILON);
	}

	#[test]
	fn step_keeps_point_count_bounded() {
		assert_eq!(step_for(60), MIN_STEP_SECS);
		let range = 30 * 86_400;
		assert!(range / step_for(range) <= MAX_POINTS);
	}
}
