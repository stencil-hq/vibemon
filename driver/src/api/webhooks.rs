//! `/events/webhooks*`: lifecycle webhook registration, delivery history,
//! and delivery stats. Delivery itself runs in [`crate::webhooks`].

use axum::{
	Json, Router,
	extract::{Path, RawQuery, State},
	http::StatusCode,
	response::IntoResponse,
	routing::get,
};
use chrono::{DateTime, Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
	auth::TeamAuth,
	error::{ApiError, ApiResult},
	state::AppState,
	store::{
		DeliveryCursor, DeliveryFilter, DeliveryRow, StatsBucket, WebhookRow, WebhookWrite,
		event_types,
	},
};

/// Default page size of the deliveries list.
const DEFAULT_DELIVERY_LIMIT: u32 = 25;
/// Maximum page size of the deliveries list.
const MAX_DELIVERY_LIMIT: u32 = 100;
/// Default stats window.
const DEFAULT_STATS_WINDOW_HOURS: i64 = 24;
/// Upper bound on stats buckets.
const MAX_STATS_BUCKETS: i64 = 100;
/// Longest webhook name / URL.
const MAX_NAME_LEN: usize = 255;
const MAX_URL_LEN: usize = 2048;

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/events/webhooks", get(list_webhooks).post(create_webhook))
		.route(
			"/events/webhooks/{webhook_id}",
			get(get_webhook)
				.patch(update_webhook)
				.delete(delete_webhook),
		)
		.route("/events/webhooks/{webhook_id}/deliveries", get(list_deliveries))
		.route("/events/webhooks/{webhook_id}/stats", get(delivery_stats))
}

/// E2B `WebhookDetail` (also `WebhookCreation`).
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WebhookDto {
	id:         String,
	team_id:    String,
	name:       String,
	created_at: DateTime<Utc>,
	url:        String,
	enabled:    bool,
	events:     Vec<String>,
}

impl From<WebhookRow> for WebhookDto {
	fn from(row: WebhookRow) -> Self {
		Self {
			id:         row.id,
			team_id:    row.team_id,
			name:       row.name,
			created_at: row.created_at,
			url:        row.url,
			enabled:    row.enabled,
			events:     row.events,
		}
	}
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WebhookCreate {
	name:             String,
	url:              String,
	events:           Vec<String>,
	enabled:          Option<bool>,
	signature_secret: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct WebhookConfiguration {
	enabled:          Option<bool>,
	name:             Option<String>,
	url:              Option<String>,
	events:           Option<Vec<String>>,
	signature_secret: Option<String>,
}

fn validate_name(name: &str) -> Result<String, ApiError> {
	let name = name.trim();
	if name.is_empty() || name.len() > MAX_NAME_LEN {
		return Err(ApiError::bad_request(format!("name must be 1-{MAX_NAME_LEN} characters")));
	}
	Ok(name.to_owned())
}

/// Accepts absolute `http`/`https` URLs only.
pub fn validate_url(raw: &str) -> Result<String, ApiError> {
	let url = url::Url::parse(raw.trim())
		.map_err(|_| ApiError::bad_request("url must be an absolute URL"))?;
	if !matches!(url.scheme(), "http" | "https") || url.host_str().is_none() {
		return Err(ApiError::bad_request("url must be an http or https URL"));
	}
	if raw.len() > MAX_URL_LEN {
		return Err(ApiError::bad_request(format!("url must be at most {MAX_URL_LEN} characters")));
	}
	Ok(url.to_string())
}

/// Accepts a non-empty set of known event types (deduplicated).
pub fn validate_events(events: &[String]) -> Result<Vec<String>, ApiError> {
	if events.is_empty() {
		return Err(ApiError::bad_request("events must not be empty"));
	}
	let mut out: Vec<String> = Vec::with_capacity(events.len());
	for event in events {
		if !event_types::ALL.contains(&event.as_str()) {
			return Err(ApiError::bad_request(format!(
				"unknown event type '{event}', expected one of: {}",
				event_types::ALL.join(", ")
			)));
		}
		if !out.contains(event) {
			out.push(event.clone());
		}
	}
	Ok(out)
}

fn validate_secret(secret: &str) -> Result<(), ApiError> {
	if secret.is_empty() || secret.len() > 512 {
		return Err(ApiError::bad_request("signatureSecret must be 1-512 characters"));
	}
	Ok(())
}

fn not_found(webhook_id: &str) -> ApiError {
	ApiError::not_found(format!("Webhook '{webhook_id}' not found"))
}

async fn list_webhooks(
	State(state): State<AppState>,
	auth: TeamAuth,
) -> ApiResult<Json<Vec<WebhookDto>>> {
	let rows = state.store.list_webhooks(&auth.team.id).await?;
	Ok(Json(rows.into_iter().map(WebhookDto::from).collect()))
}

async fn create_webhook(
	State(state): State<AppState>,
	auth: TeamAuth,
	Json(body): Json<WebhookCreate>,
) -> ApiResult<impl IntoResponse> {
	let name = validate_name(&body.name)?;
	let url = validate_url(&body.url)?;
	let events = validate_events(&body.events)?;
	validate_secret(&body.signature_secret)?;
	let secret = state.keys.encrypt_str(&body.signature_secret);
	let row = state
		.store
		.create_webhook(&auth.team.id, &name, &url, &events, body.enabled.unwrap_or(true), &secret)
		.await?;
	Ok((StatusCode::CREATED, Json(WebhookDto::from(row))))
}

async fn get_webhook(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(webhook_id): Path<String>,
) -> ApiResult<Json<WebhookDto>> {
	let row = state
		.store
		.webhook(&auth.team.id, &webhook_id)
		.await?
		.ok_or_else(|| not_found(&webhook_id))?;
	Ok(Json(row.into()))
}

async fn update_webhook(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(webhook_id): Path<String>,
	Json(body): Json<WebhookConfiguration>,
) -> ApiResult<Json<WebhookDto>> {
	let mut patch = WebhookWrite { enabled: body.enabled, ..WebhookWrite::default() };
	if let Some(name) = &body.name {
		patch.name = Some(validate_name(name)?);
	}
	if let Some(url) = &body.url {
		patch.url = Some(validate_url(url)?);
	}
	if let Some(events) = &body.events {
		patch.events = Some(validate_events(events)?);
	}
	if let Some(secret) = &body.signature_secret {
		validate_secret(secret)?;
		patch.signature_secret_enc = Some(state.keys.encrypt_str(secret));
	}
	let row = state
		.store
		.update_webhook(&auth.team.id, &webhook_id, &patch)
		.await?
		.ok_or_else(|| not_found(&webhook_id))?;
	Ok(Json(row.into()))
}

async fn delete_webhook(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(webhook_id): Path<String>,
) -> ApiResult<StatusCode> {
	if !state
		.store
		.delete_webhook(&auth.team.id, &webhook_id)
		.await?
	{
		return Err(not_found(&webhook_id));
	}
	Ok(StatusCode::OK)
}

/// E2B `WebhookDelivery`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryDto {
	id: String,
	team_id: String,
	webhook_id: String,
	event_id: String,
	sandbox_id: String,
	event_type: String,
	status: String,
	duration_ms: i32,
	request_body: String,
	request_headers: String,
	request_url: String,
	response_body: Option<String>,
	response_headers: Option<String>,
	response_http_status_code: Option<i32>,
	error_class: Option<String>,
	error_message: Option<String>,
	timestamp: DateTime<Utc>,
}

impl From<DeliveryRow> for DeliveryDto {
	fn from(r: DeliveryRow) -> Self {
		Self {
			id: r.id,
			team_id: r.team_id,
			webhook_id: r.webhook_id,
			event_id: r.event_id,
			sandbox_id: r.sandbox_id,
			event_type: r.event_type,
			status: r.status,
			duration_ms: r.duration_ms,
			request_body: r.request_body,
			request_headers: r.request_headers,
			request_url: r.request_url,
			response_body: r.response_body,
			response_headers: r.response_headers,
			response_http_status_code: r.response_status,
			error_class: r.error_class,
			error_message: r.error_message,
			timestamp: r.ts,
		}
	}
}

/// E2B `WebhookDeliveryGroup`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeliveryGroupDto {
	event_id:   String,
	event_type: String,
	sandbox_id: String,
	attempts:   Vec<DeliveryDto>,
}

/// E2B `WebhookDeliveriesListPayload`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DeliveriesPayload {
	data:        Vec<DeliveryGroupDto>,
	next_cursor: Option<String>,
}

/// Parsed deliveries/stats query.
#[derive(Debug, Default)]
pub struct DeliveryQuery {
	pub cursor:    Option<DeliveryCursor>,
	pub limit:     u32,
	pub order_asc: bool,
	pub filter:    DeliveryFilter,
}

fn parse_time(name: &str, value: &str) -> Result<DateTime<Utc>, ApiError> {
	DateTime::parse_from_rfc3339(value)
		.map(|t| t.with_timezone(&Utc))
		.map_err(|_| ApiError::bad_request(format!("{name} must be an RFC 3339 timestamp")))
}

/// Parses the deliveries/stats query string (array parameters may repeat
/// or be comma-separated).
pub fn parse_delivery_query(raw: Option<&str>) -> Result<DeliveryQuery, ApiError> {
	let mut query = DeliveryQuery { limit: DEFAULT_DELIVERY_LIMIT, ..DeliveryQuery::default() };
	for (key, value) in url::form_urlencoded::parse(raw.unwrap_or_default().as_bytes()) {
		match key.as_ref() {
			"cursor" if !value.is_empty() => {
				query.cursor = Some(
					DeliveryCursor::decode(&value)
						.ok_or_else(|| ApiError::bad_request("Invalid cursor"))?,
				);
			},
			"limit" => {
				let limit: u32 = value
					.parse()
					.map_err(|_| ApiError::bad_request("limit must be an integer"))?;
				if !(1..=MAX_DELIVERY_LIMIT).contains(&limit) {
					return Err(ApiError::bad_request(format!(
						"limit must be between 1 and {MAX_DELIVERY_LIMIT}"
					)));
				}
				query.limit = limit;
			},
			"orderAsc" => {
				query.order_asc = value
					.parse()
					.map_err(|_| ApiError::bad_request("orderAsc must be a boolean"))?;
			},
			"start" => query.filter.start = Some(parse_time("start", &value)?),
			"end" => query.filter.end = Some(parse_time("end", &value)?),
			"deliveryStatus" | "deliveryStatus[]" => {
				for status in value.split(',').map(str::trim).filter(|s| !s.is_empty()) {
					if !matches!(status, "success" | "failed") {
						return Err(ApiError::bad_request(
							"deliveryStatus must be 'success' or 'failed'",
						));
					}
					query.filter.statuses.push(status.to_owned());
				}
			},
			"eventType" | "eventType[]" => query.filter.event_types.extend(
				value
					.split(',')
					.map(str::trim)
					.filter(|s| !s.is_empty())
					.map(str::to_owned),
			),
			_ => {},
		}
	}
	if let (Some(start), Some(end)) = (query.filter.start, query.filter.end)
		&& start > end
	{
		return Err(ApiError::bad_request("start must not be after end"));
	}
	Ok(query)
}

async fn list_deliveries(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(webhook_id): Path<String>,
	RawQuery(raw): RawQuery,
) -> ApiResult<Json<DeliveriesPayload>> {
	let query = parse_delivery_query(raw.as_deref())?;
	state
		.store
		.webhook(&auth.team.id, &webhook_id)
		.await?
		.ok_or_else(|| not_found(&webhook_id))?;
	let mut groups = state
		.store
		.delivery_groups(&webhook_id, &query.filter, query.cursor, query.limit + 1, query.order_asc)
		.await?;
	let more = groups.len() > query.limit as usize;
	groups.truncate(query.limit as usize);
	let next_cursor = more
		.then(|| groups.last().map(|group| group.cursor.encode()))
		.flatten();
	let data = groups
		.into_iter()
		.filter_map(|group| {
			let first = group.attempts.first()?;
			Some(DeliveryGroupDto {
				event_type: first.event_type.clone(),
				sandbox_id: first.sandbox_id.clone(),
				event_id:   group.event_id,
				attempts:   group.attempts.into_iter().map(DeliveryDto::from).collect(),
			})
		})
		.collect();
	Ok(Json(DeliveriesPayload { data, next_cursor }))
}

/// E2B `WebhookDeliveryDurationStats`.
#[derive(Serialize, Debug, PartialEq)]
pub struct DurationStats {
	minimum: f64,
	average: f64,
	maximum: f64,
}

/// E2B `WebhookDeliveryStatsBucket`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsBucketDto {
	timestamp:   DateTime<Utc>,
	total:       i64,
	failed:      i64,
	duration_ms: DurationStats,
}

/// E2B `WebhookDeliveryStats`.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct StatsDto {
	buckets:     Vec<StatsBucketDto>,
	total:       i64,
	failed:      i64,
	duration_ms: DurationStats,
}

/// Overall totals and duration stats across buckets (average weighted by
/// attempt count).
pub fn summarize(buckets: &[StatsBucket]) -> (i64, i64, DurationStats) {
	let total: i64 = buckets.iter().map(|b| b.total).sum();
	let failed: i64 = buckets.iter().map(|b| b.failed).sum();
	if total == 0 {
		return (0, 0, DurationStats { minimum: 0.0, average: 0.0, maximum: 0.0 });
	}
	#[allow(clippy::cast_precision_loss, reason = "attempt counts are far below 2^52")]
	let average = buckets
		.iter()
		.map(|b| b.avg_ms * b.total as f64)
		.sum::<f64>()
		/ total as f64;
	let minimum = buckets
		.iter()
		.map(|b| b.min_ms)
		.fold(f64::INFINITY, f64::min);
	let maximum = buckets.iter().map(|b| b.max_ms).fold(0.0, f64::max);
	(total, failed, DurationStats { minimum, average, maximum })
}

async fn delivery_stats(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(webhook_id): Path<String>,
	RawQuery(raw): RawQuery,
) -> ApiResult<Json<StatsDto>> {
	let mut query = parse_delivery_query(raw.as_deref())?;
	state
		.store
		.webhook(&auth.team.id, &webhook_id)
		.await?
		.ok_or_else(|| not_found(&webhook_id))?;
	let end = query.filter.end.unwrap_or_else(Utc::now);
	let start = query
		.filter
		.start
		.unwrap_or(end - Duration::hours(DEFAULT_STATS_WINDOW_HOURS));
	query.filter.start = Some(start);
	query.filter.end = Some(end);
	let range = (end - start).num_seconds().max(1);
	let bucket_secs = ((range + MAX_STATS_BUCKETS - 1) / MAX_STATS_BUCKETS).max(60);
	let buckets = state
		.store
		.delivery_stats(&webhook_id, &query.filter, bucket_secs)
		.await?;
	let (total, failed, duration_ms) = summarize(&buckets);
	Ok(Json(StatsDto {
		buckets: buckets
			.into_iter()
			.map(|b| StatsBucketDto {
				timestamp:   b.start,
				total:       b.total,
				failed:      b.failed,
				duration_ms: DurationStats { minimum: b.min_ms, average: b.avg_ms, maximum: b.max_ms },
			})
			.collect(),
		total,
		failed,
		duration_ms,
	}))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn url_and_event_validation() {
		assert!(validate_url("https://example.com/hook").is_ok());
		assert!(validate_url("ftp://example.com").is_err());
		assert!(validate_url("/relative").is_err());
		assert_eq!(
			validate_events(&[event_types::KILLED.into(), event_types::KILLED.into()]).unwrap(),
			vec![event_types::KILLED]
		);
		assert!(validate_events(&[]).is_err());
		assert!(validate_events(&["sandbox.lifecycle.exploded".into()]).is_err());
	}

	#[test]
	fn delivery_query_parsing() {
		let q = parse_delivery_query(Some(
			"limit=5&orderAsc=true&deliveryStatus=failed&deliveryStatus=success&eventType=a,b&\
			 start=2025-01-01T00:00:00Z",
		))
		.unwrap();
		assert_eq!((q.limit, q.order_asc), (5, true));
		assert_eq!(q.filter.statuses, vec!["failed", "success"]);
		assert_eq!(q.filter.event_types, vec!["a", "b"]);
		assert!(parse_delivery_query(Some("deliveryStatus=pending")).is_err());
		assert!(parse_delivery_query(Some("limit=101")).is_err());
		assert!(
			parse_delivery_query(Some("start=2025-02-01T00:00:00Z&end=2025-01-01T00:00:00Z")).is_err()
		);
		assert!(parse_delivery_query(Some("cursor=%%%")).is_err());
	}

	#[test]
	fn summary_weights_average_by_attempts() {
		let at = Utc::now();
		let buckets = [
			StatsBucket {
				start:  at,
				total:  1,
				failed: 1,
				min_ms: 100.0,
				avg_ms: 100.0,
				max_ms: 100.0,
			},
			StatsBucket { start: at, total: 3, failed: 0, min_ms: 10.0, avg_ms: 20.0, max_ms: 30.0 },
		];
		let (total, failed, d) = summarize(&buckets);
		assert_eq!((total, failed), (4, 1));
		assert_eq!(d, DurationStats { minimum: 10.0, average: 40.0, maximum: 100.0 });
		assert_eq!(summarize(&[]).0, 0);
	}
}
