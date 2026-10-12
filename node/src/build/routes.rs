//! Node API routes for builds and artifacts (ARCHITECTURE.md §8.5), relative
//! to `/node/v1`. Authentication is applied by the node server.

use axum::{
	Json, Router,
	body::Body,
	extract::{Path, Query, State},
	http::{HeaderMap, StatusCode, header},
	response::{IntoResponse, Response},
	routing::{get, post},
};
use serde::Deserialize;
use tokio::io::{AsyncReadExt, AsyncSeekExt};

use super::{Builder, StartError};
use crate::api::{ErrorBody, StartBuild};

/// Router for `POST /builds`, `GET|DELETE /builds/{id}`,
/// `GET /artifacts/{id}/manifest`, `GET /artifacts/{id}/files/{*path}`, and
/// `DELETE /artifacts/{id}`; the node server nests it under `/node/v1`.
pub fn routes(builder: Builder) -> Router {
	Router::new()
		.route("/builds", post(start_build))
		.route("/builds/{id}", get(build_status).delete(cancel_build))
		.route("/artifacts/{id}", axum::routing::delete(delete_artifact))
		.route("/artifacts/{id}/manifest", get(artifact_manifest))
		.route("/artifacts/{id}/files/{*path}", get(artifact_file))
		.with_state(builder)
}

fn error(status: StatusCode, code: &str, message: impl Into<String>) -> Response {
	(status, Json(ErrorBody { code: code.to_owned(), message: message.into() })).into_response()
}

fn internal(error: &anyhow::Error) -> Response {
	error_response_for(error)
}

fn error_response_for(error: &anyhow::Error) -> Response {
	let message = format!("{error:#}");
	if message.starts_with("invalid build id") {
		return self::error(StatusCode::BAD_REQUEST, "invalid", message);
	}
	self::error(StatusCode::INTERNAL_SERVER_ERROR, "internal", message)
}

async fn start_build(State(builder): State<Builder>, Json(req): Json<StartBuild>) -> Response {
	match builder.start(req) {
		Ok(()) => StatusCode::ACCEPTED.into_response(),
		Err(StartError::Invalid(message)) => error(StatusCode::BAD_REQUEST, "invalid", message),
		Err(StartError::Internal(e)) => internal(&e),
	}
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct StatusQuery {
	#[serde(default)]
	logs_offset: u64,
}

async fn build_status(
	State(builder): State<Builder>,
	Path(id): Path<String>,
	Query(query): Query<StatusQuery>,
) -> Response {
	match builder.status(&id, query.logs_offset) {
		Some(status) => Json(status).into_response(),
		None => error(StatusCode::NOT_FOUND, "not_found", format!("build {id} not found")),
	}
}

async fn cancel_build(State(builder): State<Builder>, Path(id): Path<String>) -> Response {
	if builder.cancel(&id) {
		StatusCode::NO_CONTENT.into_response()
	} else {
		error(StatusCode::NOT_FOUND, "not_found", format!("build {id} is not running on this node"))
	}
}

async fn artifact_manifest(State(builder): State<Builder>, Path(id): Path<String>) -> Response {
	match builder.artifacts().manifest(&id).await {
		Ok(Some(manifest)) => Json(manifest).into_response(),
		Ok(None) => error(StatusCode::NOT_FOUND, "not_found", format!("no artifacts for build {id}")),
		Err(e) => internal(&e),
	}
}

async fn delete_artifact(State(builder): State<Builder>, Path(id): Path<String>) -> Response {
	match builder.artifacts().delete(&id).await {
		Ok(_) => StatusCode::NO_CONTENT.into_response(),
		Err(e) => internal(&e),
	}
}

/// Query of `GET /artifacts/{id}/files/{*path}`.
#[derive(Debug, Default, Deserialize)]
struct FileQuery {
	/// Stream only the manifest's data extents, concatenated in order.
	#[serde(default)]
	sparse: bool,
}

async fn artifact_file(
	State(builder): State<Builder>,
	Path((id, path)): Path<(String, String)>,
	Query(query): Query<FileQuery>,
	headers: HeaderMap,
) -> Response {
	let (file_path, entry) = match builder.artifacts().served_file(&id, &path).await {
		Ok(Some(found)) => found,
		Ok(None) => {
			return error(
				StatusCode::NOT_FOUND,
				"not_found",
				format!("{path} is not an artifact of {id}"),
			);
		},
		Err(e) => return internal(&e),
	};
	if query.sparse {
		let len: u64 = entry.extents.iter().map(|e| e[1]).sum();
		let stream = crate::artifacts::sparse::read_extents(file_path, entry.extents);
		return Response::builder()
			.header(header::CONTENT_TYPE, "application/octet-stream")
			.header(header::CONTENT_LENGTH, len)
			.body(Body::from_stream(stream))
			.unwrap_or_default();
	}
	let mut file = match tokio::fs::File::open(&file_path).await {
		Ok(f) => f,
		Err(e) => return error(StatusCode::NOT_FOUND, "not_found", e.to_string()),
	};
	let size = match file.metadata().await {
		Ok(m) => m.len(),
		Err(e) => return internal(&e.into()),
	};
	let range = headers.get(header::RANGE).and_then(|v| v.to_str().ok());
	let (status, start, len) = match range.map(|r| parse_range(r, size)) {
		None => (StatusCode::OK, 0, size),
		Some(Some((start, end))) => (StatusCode::PARTIAL_CONTENT, start, end - start + 1),
		Some(None) => {
			return Response::builder()
				.status(StatusCode::RANGE_NOT_SATISFIABLE)
				.header(header::CONTENT_RANGE, format!("bytes */{size}"))
				.body(Body::empty())
				.unwrap_or_default();
		},
	};
	if start > 0
		&& let Err(e) = file.seek(std::io::SeekFrom::Start(start)).await
	{
		return internal(&e.into());
	}
	let stream = tokio_util::io::ReaderStream::with_capacity(file.take(len), 256 * 1024);
	let mut response = Response::builder()
		.status(status)
		.header(header::CONTENT_TYPE, "application/octet-stream")
		.header(header::CONTENT_LENGTH, len)
		.header(header::ACCEPT_RANGES, "bytes");
	if status == StatusCode::PARTIAL_CONTENT {
		response = response
			.header(header::CONTENT_RANGE, format!("bytes {start}-{}/{size}", start + len - 1));
	}
	response.body(Body::from_stream(stream)).unwrap_or_default()
}

/// Parses a single `bytes=` range against `size`, returning the inclusive
/// `(start, end)`; `None` when unsatisfiable or malformed (multi-range too).
pub fn parse_range(header: &str, size: u64) -> Option<(u64, u64)> {
	let spec = header.trim().strip_prefix("bytes=")?;
	if spec.contains(',') || size == 0 {
		return None;
	}
	let (start, end) = spec.split_once('-')?;
	let (start, end) = (start.trim(), end.trim());
	if start.is_empty() {
		let suffix: u64 = end.parse().ok()?;
		if suffix == 0 {
			return None;
		}
		return Some((size.saturating_sub(suffix), size - 1));
	}
	let start: u64 = start.parse().ok()?;
	let end = if end.is_empty() {
		size - 1
	} else {
		end.parse::<u64>().ok()?.min(size - 1)
	};
	(start <= end && start < size).then_some((start, end))
}

#[cfg(test)]
mod tests {
	use super::parse_range;

	#[test]
	fn ranges() {
		assert_eq!(parse_range("bytes=0-99", 1000), Some((0, 99)));
		assert_eq!(parse_range("bytes=900-", 1000), Some((900, 999)));
		assert_eq!(parse_range("bytes=-100", 1000), Some((900, 999)));
		assert_eq!(
			parse_range("bytes=-5000", 1000),
			Some((0, 999)),
			"suffix larger than file = whole file"
		);
		assert_eq!(parse_range("bytes=990-5000", 1000), Some((990, 999)), "end is clamped");
		assert_eq!(parse_range("bytes=1000-", 1000), None, "start past the end");
		assert_eq!(parse_range("bytes=5-4", 1000), None);
		assert_eq!(parse_range("bytes=-0", 1000), None);
		assert_eq!(parse_range("bytes=0-1,5-6", 1000), None, "multi-range unsupported");
		assert_eq!(parse_range("items=0-1", 1000), None);
		assert_eq!(parse_range("bytes=0-0", 0), None);
	}
}
