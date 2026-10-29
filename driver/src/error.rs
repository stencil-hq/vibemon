//! E2B-shaped API errors: every non-2xx response body is
//! `{"code": <http status>, "message": "...", "error_code"?: "..."}`.

use axum::{
	Json,
	http::{HeaderValue, StatusCode, header},
	response::{IntoResponse, Response},
};
use serde::Serialize;
use vmon_node::client::NodeError;

/// API failure rendered in the E2B `Error` schema.
#[derive(Debug, Clone)]
pub struct ApiError {
	pub status:      StatusCode,
	pub message:     String,
	/// Machine-readable semantic code (`sandbox_capacity_unavailable`, …).
	pub error_code:  Option<&'static str>,
	/// Seconds for `Retry-After` on 429/503.
	pub retry_after: Option<u64>,
}

#[derive(Serialize)]
struct Body<'a> {
	code:       u16,
	message:    &'a str,
	#[serde(skip_serializing_if = "Option::is_none")]
	error_code: Option<&'static str>,
}

impl ApiError {
	pub fn new(status: StatusCode, message: impl Into<String>) -> Self {
		Self { status, message: message.into(), error_code: None, retry_after: None }
	}

	pub const fn with_code(mut self, error_code: &'static str) -> Self {
		self.error_code = Some(error_code);
		self
	}

	pub fn bad_request(message: impl Into<String>) -> Self {
		Self::new(StatusCode::BAD_REQUEST, message)
	}

	pub fn unauthorized(message: impl Into<String>) -> Self {
		Self::new(StatusCode::UNAUTHORIZED, message)
	}

	pub fn forbidden(message: impl Into<String>) -> Self {
		Self::new(StatusCode::FORBIDDEN, message)
	}

	pub fn not_found(message: impl Into<String>) -> Self {
		Self::new(StatusCode::NOT_FOUND, message)
	}

	pub fn conflict(message: impl Into<String>) -> Self {
		Self::new(StatusCode::CONFLICT, message)
	}

	pub fn gone(message: impl Into<String>) -> Self {
		Self::new(StatusCode::GONE, message)
	}

	pub fn unprocessable(message: impl Into<String>) -> Self {
		Self::new(StatusCode::UNPROCESSABLE_ENTITY, message)
	}

	pub fn too_many_requests(message: impl Into<String>, retry_after_secs: u64) -> Self {
		Self {
			retry_after: Some(retry_after_secs),
			..Self::new(StatusCode::TOO_MANY_REQUESTS, message)
		}
	}

	pub fn internal(message: impl Into<String>) -> Self {
		Self::new(StatusCode::INTERNAL_SERVER_ERROR, message).with_code("internal_server_error")
	}

	pub fn not_implemented(message: impl Into<String>) -> Self {
		Self::new(StatusCode::NOT_IMPLEMENTED, message)
	}

	pub fn unavailable(message: impl Into<String>) -> Self {
		Self::new(StatusCode::SERVICE_UNAVAILABLE, message)
	}

	/// Upstream wording for unknown or foreign sandboxes.
	pub fn sandbox_not_found(sandbox_id: &str) -> Self {
		Self::not_found(format!(
			"Sandbox \"{sandbox_id}\" doesn't exist or you don't have access to it"
		))
	}
}

impl IntoResponse for ApiError {
	fn into_response(self) -> Response {
		let body = Body {
			code:       self.status.as_u16(),
			message:    &self.message,
			error_code: self.error_code,
		};
		let mut response = (self.status, Json(body)).into_response();
		if let Some(secs) = self.retry_after
			&& let Ok(value) = HeaderValue::from_str(&secs.to_string())
		{
			response.headers_mut().insert(header::RETRY_AFTER, value);
		}
		response
	}
}

impl From<crate::store::Error> for ApiError {
	fn from(error: crate::store::Error) -> Self {
		use vmon_cluster::kv::KvError;

		use crate::store::Error;
		match error {
			Error::Kv(error @ (KvError::NoLeader | KvError::Unavailable(_))) => error.into(),
			Error::Duplicate(what) => Self::conflict(format!("{what} already exists")),
			error => {
				tracing::error!(%error, "database error");
				Self::internal("database error")
			},
		}
	}
}

impl From<vmon_cluster::kv::KvError> for ApiError {
	fn from(error: vmon_cluster::kv::KvError) -> Self {
		tracing::warn!(%error, "kv error");
		Self::unavailable(format!("cluster state unavailable: {error}"))
	}
}

impl From<NodeError> for ApiError {
	fn from(error: NodeError) -> Self {
		match &error {
			NodeError::Api { status: 404, .. } => Self::not_found(error.to_string()),
			NodeError::Api { code, message, .. }
				if code == vmon_node::api::NESTED_VIRTUALIZATION_ACTIVE =>
			{
				Self::conflict(message.clone()).with_code(vmon_node::api::NESTED_VIRTUALIZATION_ACTIVE)
			},
			NodeError::Api { status: 409, message, .. } => Self::conflict(message.clone()),
			NodeError::Api { status: 400, message, .. } => Self::bad_request(message.clone()),
			NodeError::Api { code, .. } if code == "busy" => {
				Self::unavailable(error.to_string()).with_code("sandbox_capacity_unavailable")
			},
			_ => {
				tracing::warn!(%error, "node call failed");
				Self::internal(error.to_string())
			},
		}
	}
}

/// Handler result alias.
pub type ApiResult<T> = Result<T, ApiError>;

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn nested_virtualization_refusals_keep_their_code() {
		let error = ApiError::from(NodeError::Api {
			status:  409,
			code:    vmon_node::api::NESTED_VIRTUALIZATION_ACTIVE.to_owned(),
			message: "sandbox x is running virtual machines".to_owned(),
		});
		assert_eq!(error.status, StatusCode::CONFLICT);
		assert_eq!(error.error_code, Some("nested_virtualization_active"));
		assert_eq!(error.message, "sandbox x is running virtual machines");
	}
}
