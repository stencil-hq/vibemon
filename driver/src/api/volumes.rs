//! `/volumes` CRUD and the volume-content API.
//!
//! CRUD covers placement on a node or the operator object store + volume
//! JWTs. The volume-content API (`/volumecontent/{id}/{path,dir,file}`) is
//! served by the driver directly
//! against S3 for object-store volumes, otherwise proxied to the owning
//! node's `/node/v1/volumes/{id}/content/…`.

use axum::{
	Json, Router,
	body::Body,
	extract::{Path, Request, State},
	http::{HeaderMap, HeaderValue, Method, StatusCode, header},
	response::{IntoResponse, Response},
	routing::{any, get},
};
use futures::TryStreamExt;
use serde::{Deserialize, Serialize};
use vmon_cloud::s3::S3Client;
use vmon_cluster::NodeId;
use vmon_node::{client::NodeClient, s3fs, volumes::ContentQuery};

use crate::{
	auth::TeamAuth,
	error::{ApiError, ApiResult},
	state::AppState,
	store::VolumeRow,
};

/// Lifetime of volume-content tokens.
const VOLUME_TOKEN_TTL_SECS: i64 = 24 * 3600;
/// Longest volume name.
const MAX_NAME_LEN: usize = 255;
/// Request headers forwarded to the node for content operations.
const FORWARD_REQUEST_HEADERS: [header::HeaderName; 2] =
	[header::CONTENT_TYPE, header::CONTENT_LENGTH];
/// Response headers copied back from the node.
const FORWARD_RESPONSE_HEADERS: [header::HeaderName; 4] = [
	header::CONTENT_TYPE,
	header::CONTENT_LENGTH,
	header::CONTENT_DISPOSITION,
	header::LAST_MODIFIED,
];

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/volumes", get(list_volumes).post(create_volume))
		.route("/volumes/{volume_id}", get(get_volume).delete(delete_volume))
		.route("/volumecontent/{volume_id}/{op}", any(volume_content))
}

/// E2B `Volume`.
#[derive(Serialize)]
struct VolumeDto {
	#[serde(rename = "volumeID")]
	volume_id: String,
	name:      String,
}

/// E2B `VolumeAndToken`.
#[derive(Serialize)]
struct VolumeAndTokenDto {
	#[serde(rename = "volumeID")]
	volume_id: String,
	name:      String,
	token:     String,
}

#[derive(Deserialize)]
struct NewVolume {
	name: String,
}

fn validate_name(name: &str) -> Result<(), ApiError> {
	if name.is_empty() || name.len() > MAX_NAME_LEN {
		return Err(ApiError::bad_request(format!(
			"Volume name must be 1-{MAX_NAME_LEN} characters"
		)));
	}
	if !name
		.bytes()
		.all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
	{
		return Err(ApiError::bad_request("Volume name must match ^[a-zA-Z0-9_-]+$"));
	}
	Ok(())
}

fn with_token(state: &AppState, row: VolumeRow) -> VolumeAndTokenDto {
	let token = state
		.keys
		.volume_token(&row.team_id, &row.id, VOLUME_TOKEN_TTL_SECS);
	VolumeAndTokenDto { volume_id: row.id, name: row.name, token }
}

/// The alive, non-draining node with the most free disk.
fn pick_volume_node(state: &AppState) -> Result<(NodeId, String), ApiError> {
	state
		.node_members()
		.into_iter()
		.filter_map(|(id, meta)| {
			meta
				.node
				.filter(|cap| !cap.draining)
				.map(|cap| (id, meta.cluster_url, cap.disk_free_mb))
		})
		.max_by_key(|(id, _, free)| (*free, std::cmp::Reverse(id.0)))
		.map(|(id, url, _)| (id, url))
		.ok_or_else(|| ApiError::unavailable("No node is available to host the volume"))
}

/// Client scoped to an object-store volume's `volumes/<id>/` prefix.
fn volume_store(state: &AppState, volume: &VolumeRow) -> Result<S3Client, ApiError> {
	state
		.config
		.object_store
		.as_ref()
		.ok_or_else(|| {
			ApiError::unavailable(format!(
				"volume {} lives in the object store, which this driver is not configured with",
				volume.id
			))
		})?
		.with_prefix(&s3fs::volume_prefix(&volume.id))
		.map_err(|error| ApiError::internal(format!("volume {}: {error}", volume.id)))
}

/// Client for the node that serves `volume` (any alive node for shared
/// volumes).
fn volume_node(state: &AppState, volume: &VolumeRow) -> Result<NodeClient, ApiError> {
	if let Some(node) = &volume.node_id {
		let id: NodeId = node.parse().map_err(|_| {
			ApiError::internal(format!("volume {} has a malformed node id", volume.id))
		})?;
		state.node(id)
	} else {
		let (_, url) = pick_volume_node(state)?;
		state.node_at(&url)
	}
}

/// Sends a body-less node call; `Missing::Ok` treats 404 as success.
async fn node_call(
	node: &NodeClient,
	method: Method,
	path: &str,
	tolerate_missing: Missing,
) -> Result<(), ApiError> {
	let response = node
		.forward(method, path, HeaderMap::new(), reqwest::Body::from(Vec::new()))
		.await?;
	let status = response.status();
	if status.is_success() || (tolerate_missing == Missing::Ok && status == StatusCode::NOT_FOUND) {
		return Ok(());
	}
	let body = response.text().await.unwrap_or_default();
	Err(ApiError::internal(format!("node volume operation failed ({status}): {body}")))
}

/// Whether a 404 from the node counts as success.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Missing {
	Ok,
	Error,
}

async fn list_volumes(
	State(state): State<AppState>,
	auth: TeamAuth,
) -> ApiResult<Json<Vec<VolumeDto>>> {
	let rows = state.store.list_volumes(&auth.team.id).await?;
	Ok(Json(
		rows
			.into_iter()
			.map(|r| VolumeDto { volume_id: r.id, name: r.name })
			.collect(),
	))
}

async fn create_volume(
	State(state): State<AppState>,
	auth: TeamAuth,
	Json(body): Json<NewVolume>,
) -> ApiResult<Response> {
	validate_name(&body.name)?;
	// With an object store, volumes are prefixes there: no node owns them.
	let placed = if state.config.object_store.is_some() {
		None
	} else {
		Some(pick_volume_node(&state)?)
	};
	let owner = placed
		.as_ref()
		.filter(|_| !state.config.shared_volumes)
		.map(|(node_id, _)| node_id.to_string());
	let row = match state
		.store
		.insert_volume(&auth.team.id, &body.name, owner.as_deref(), placed.is_none())
		.await
	{
		Ok(row) => row,
		Err(sqlx::Error::Database(e)) if e.is_unique_violation() => {
			return Err(ApiError::conflict(format!(
				"Volume with name '{}' already exists",
				body.name
			)));
		},
		Err(e) => return Err(e.into()),
	};
	let Some((_, node_url)) = placed else {
		return Ok((StatusCode::CREATED, Json(with_token(&state, row))).into_response());
	};
	let node = state.node_at(&node_url)?;
	if let Err(error) =
		node_call(&node, Method::POST, &format!("/volumes/{}", row.id), Missing::Error).await
	{
		if let Err(cleanup) = state.store.delete_volume(&auth.team.id, &row.id).await {
			tracing::warn!(%cleanup, volume = %row.id, "removing volume row after failed placement");
		}
		return Err(error);
	}
	Ok((StatusCode::CREATED, Json(with_token(&state, row))).into_response())
}

async fn get_volume(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(volume_id): Path<String>,
) -> ApiResult<Json<VolumeAndTokenDto>> {
	let row = state
		.store
		.volume(&auth.team.id, &volume_id)
		.await?
		.ok_or_else(|| ApiError::not_found(format!("Volume '{volume_id}' not found")))?;
	Ok(Json(with_token(&state, row)))
}

async fn delete_volume(
	State(state): State<AppState>,
	auth: TeamAuth,
	Path(volume_id): Path<String>,
) -> ApiResult<StatusCode> {
	let row = state
		.store
		.volume(&auth.team.id, &volume_id)
		.await?
		.ok_or_else(|| ApiError::not_found(format!("Volume '{volume_id}' not found")))?;
	if row.object_store {
		let removed = volume_store(&state, &row)?
			.delete_prefix("")
			.await
			.map_err(|error| ApiError::internal(format!("deleting volume {}: {error}", row.id)))?;
		tracing::info!(volume = %row.id, objects = removed, "deleted object-store volume");
		state.store.delete_volume(&auth.team.id, &row.id).await?;
		return Ok(StatusCode::NO_CONTENT);
	}
	match volume_node(&state, &row) {
		Ok(node) => {
			node_call(&node, Method::DELETE, &format!("/volumes/{}", row.id), Missing::Ok).await?;
		},
		// The owning node is gone: its data is unreachable, drop the record.
		Err(error) => {
			tracing::warn!(volume = %row.id, message = %error.message, "deleting volume of unavailable node");
		},
	}
	state.store.delete_volume(&auth.team.id, &row.id).await?;
	Ok(StatusCode::NO_CONTENT)
}

/// Error body of the volume-content API (`code` is a string there).
struct ContentError {
	status:  StatusCode,
	code:    &'static str,
	message: String,
}

impl ContentError {
	fn new(status: StatusCode, code: &'static str, message: impl Into<String>) -> Self {
		Self { status, code, message: message.into() }
	}
}

impl IntoResponse for ContentError {
	fn into_response(self) -> Response {
		(self.status, Json(serde_json::json!({"code": self.code, "message": self.message})))
			.into_response()
	}
}

impl From<ApiError> for ContentError {
	fn from(error: ApiError) -> Self {
		let code = match error.status {
			StatusCode::NOT_FOUND => "not_found",
			StatusCode::SERVICE_UNAVAILABLE => "unavailable",
			StatusCode::BAD_REQUEST => "bad_request",
			_ => "internal",
		};
		Self { status: error.status, code, message: error.message }
	}
}

impl From<sqlx::Error> for ContentError {
	fn from(error: sqlx::Error) -> Self {
		ApiError::from(error).into()
	}
}

/// Checks the volume token in `headers` against `volume_id`; returns the
/// token's team id.
fn authorize_content(
	state: &AppState,
	headers: &HeaderMap,
	volume_id: &str,
) -> Result<String, ContentError> {
	let token = headers
		.get(header::AUTHORIZATION)
		.and_then(|v| v.to_str().ok())
		.and_then(|v| {
			v.split_once(' ')
				.filter(|(scheme, _)| scheme.eq_ignore_ascii_case("bearer"))
				.map(|(_, t)| t.trim())
		})
		.ok_or_else(|| {
			ContentError::new(StatusCode::UNAUTHORIZED, "unauthorized", "missing volume token")
		})?;
	let claims = state.keys.verify_volume_token(token).ok_or_else(|| {
		ContentError::new(StatusCode::UNAUTHORIZED, "unauthorized", "invalid or expired volume token")
	})?;
	if claims.volid != volume_id {
		return Err(ContentError::new(
			StatusCode::FORBIDDEN,
			"forbidden",
			"token is not valid for this volume",
		));
	}
	Ok(claims.teamid)
}

/// `ANY /volumecontent/{volumeID}/{path|dir|file}`.
async fn volume_content(
	State(state): State<AppState>,
	Path((volume_id, op)): Path<(String, String)>,
	request: Request,
) -> Result<Response, ContentError> {
	if !matches!(op.as_str(), "path" | "dir" | "file") {
		return Err(ContentError::new(
			StatusCode::NOT_FOUND,
			"not_found",
			format!("unknown operation '{op}'"),
		));
	}
	let team_id = authorize_content(&state, request.headers(), &volume_id)?;
	let volume = state
		.store
		.volume(&team_id, &volume_id)
		.await?
		.ok_or_else(|| {
			ContentError::new(
				StatusCode::NOT_FOUND,
				"not_found",
				format!("volume '{volume_id}' not found"),
			)
		})?;
	if volume.object_store {
		let client = volume_store(&state, &volume)?;
		let (parts, body) = request.into_parts();
		let axum::extract::Query(query) =
			axum::extract::Query::<ContentQuery>::try_from_uri(&parts.uri).map_err(|error| {
				ContentError::new(StatusCode::BAD_REQUEST, "invalid", error.body_text())
			})?;
		let length = parts
			.headers
			.get(header::CONTENT_LENGTH)
			.and_then(|value| value.to_str().ok())
			.and_then(|value| value.parse().ok());
		return s3fs::content::serve(client, op, parts.method, query, length, body)
			.await
			.or_else(|error| Ok(error.into_response()));
	}
	let node = volume_node(&state, &volume)?;
	let (parts, body) = request.into_parts();
	let mut path = format!("/volumes/{}/content/{op}", volume.id);
	if let Some(query) = parts.uri.query() {
		path.push('?');
		path.push_str(query);
	}
	let mut headers = HeaderMap::new();
	for name in FORWARD_REQUEST_HEADERS {
		if let Some(value) = parts.headers.get(&name) {
			headers.insert(name, value.clone());
		}
	}
	let body = reqwest::Body::wrap_stream(body.into_data_stream());
	let upstream = node
		.forward(parts.method, &path, headers, body)
		.await
		.map_err(ApiError::from)?;
	let mut response = Response::builder().status(upstream.status().as_u16());
	for name in FORWARD_RESPONSE_HEADERS {
		if let Some(value) = upstream.headers().get(&name)
			&& let Ok(value) = HeaderValue::from_bytes(value.as_bytes())
		{
			response = response.header(name, value);
		}
	}
	let stream = upstream.bytes_stream().map_err(std::io::Error::other);
	response
		.body(Body::from_stream(stream))
		.map_err(|e| ContentError::new(StatusCode::INTERNAL_SERVER_ERROR, "internal", e.to_string()))
}

#[cfg(test)]
mod tests {
	use super::validate_name;

	#[test]
	fn volume_names_follow_the_spec_pattern() {
		assert!(validate_name("data_1-x").is_ok());
		assert!(validate_name("").is_err());
		assert!(validate_name("a/b").is_err());
		assert!(validate_name("a b").is_err());
	}
}
