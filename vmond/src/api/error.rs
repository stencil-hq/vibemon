use axum::{
	Json,
	http::{HeaderValue, StatusCode, header},
	response::{IntoResponse, Response},
};
use serde::Serialize;

use crate::{EngineError, ErrorCode};

fn error_guidance(code: &str) -> (bool, &'static str) {
	match code {
		"invalid" => (false, "correct the request parameters"),
		"unauthorized" => (false, "provide a valid bearer token"),
		"not_found" => (false, "verify the resource identifier"),
		"not_running" => (false, "restart or recreate the sandbox"),
		"actor_lost" => (false, "recreate the actor"),
		"unavailable_secret" => (false, "supply the required secrets"),
		"checksum" => (false, "upload the artifact again"),
		"unsupported" => (false, "choose a supported configuration"),
		"busy" | "conflict" => (true, "retry after the current operation completes"),
		"deadline" => (true, "retry with a longer deadline"),
		"ha_unavailable" => (true, "restore quorum or eligible capacity, then retry"),
		_ => (true, "retry; inspect server logs if the error persists"),
	}
}

/// JSON error document returned by HTTP endpoints.
#[derive(Clone, Debug, Serialize)]
pub struct ErrorBody {
	/// Stable machine-readable error code.
	pub code:      String,
	/// Human-readable failure detail.
	pub message:   String,
	/// Whether repeating the operation may succeed without changing its input.
	pub retryable: bool,
	/// Concrete operator or caller action for recovery.
	pub action:    String,
}

/// Transport-neutral API failure with a stable recovery contract.
#[derive(Clone, Debug)]
pub struct ApiError {
	status:       StatusCode,
	body:         ErrorBody,
	authenticate: bool,
}

/// API result used by HTTP handlers.
pub type ApiResult<T> = std::result::Result<T, ApiError>;

impl ApiError {
	/// Create an API error and derive retry guidance from its stable code.
	pub fn new(status: StatusCode, code: impl Into<String>, message: impl Into<String>) -> Self {
		let code = code.into();
		let (retryable, action) = error_guidance(&code);
		Self {
			status,
			body: ErrorBody { code, message: message.into(), retryable, action: action.to_owned() },
			authenticate: false,
		}
	}

	/// Report invalid caller input.
	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new(StatusCode::BAD_REQUEST, "invalid", message)
	}

	/// Return the stable machine-readable code.
	pub fn code(&self) -> &str {
		&self.body.code
	}

	/// Return the human-readable failure detail.
	pub fn message(&self) -> &str {
		&self.body.message
	}

	/// Return the HTTP status used by non-gRPC transports.
	pub const fn status(&self) -> StatusCode {
		self.status
	}

	/// Return whether the same operation can succeed without new input.
	pub const fn retryable(&self) -> bool {
		self.body.retryable
	}

	/// Return the recommended recovery action.
	pub fn action(&self) -> &str {
		&self.body.action
	}

	/// Construct an API error from a stable function-runtime error code.
	///
	/// The HTTP status mirrors the gRPC mapping in `api::grpc::status_from`;
	/// callers retain the stable code in either response representation.
	pub fn function(code: impl Into<String>, message: impl Into<String>) -> Self {
		let code = code.into();
		let status = match code.as_str() {
			"not_found" => StatusCode::NOT_FOUND,
			"invalid" | "checksum" => StatusCode::BAD_REQUEST,
			"unauthorized" => StatusCode::UNAUTHORIZED,
			"actor_lost" | "unavailable_secret" | "ha_unavailable" => StatusCode::PRECONDITION_FAILED,
			"busy" | "conflict" => StatusCode::CONFLICT,
			"deadline" => StatusCode::GATEWAY_TIMEOUT,
			"unsupported" => StatusCode::NOT_IMPLEMENTED,
			_ => StatusCode::SERVICE_UNAVAILABLE,
		};
		Self::new(status, code, message)
	}

	/// Report a missing or invalid bearer credential.
	pub fn unauthorized(message: impl Into<String>) -> Self {
		Self { authenticate: true, ..Self::new(StatusCode::UNAUTHORIZED, "unauthorized", message) }
	}

	/// Report a credential without permission for the operation.
	pub fn forbidden(message: impl Into<String>) -> Self {
		Self::new(StatusCode::FORBIDDEN, "unauthorized", message)
	}

	/// Report a failed request to an owning mesh node.
	pub fn bad_gateway(code: impl Into<String>, message: impl Into<String>) -> Self {
		Self::new(StatusCode::BAD_GATEWAY, code, message)
	}
}

impl From<EngineError> for ApiError {
	fn from(value: EngineError) -> Self {
		if value.message.starts_with("ha_unavailable") {
			return Self::function("ha_unavailable", value.message);
		}
		if value.message.starts_with("actor_lost") || value.message.contains("actor is lost") {
			return Self::function("actor_lost", value.message);
		}
		if value.message.starts_with("secret_unavailable")
			|| value.message.starts_with("unavailable_secret")
		{
			return Self::function("unavailable_secret", value.message);
		}
		if value.message.contains("checksum mismatch") || value.message.contains("digest mismatch") {
			return Self::function("checksum", value.message);
		}
		if value.message.contains("deadline exceeded") {
			return Self::function("deadline", value.message);
		}
		let status = match value.code {
			ErrorCode::NotFound => StatusCode::NOT_FOUND,
			ErrorCode::NotRunning | ErrorCode::Busy => StatusCode::CONFLICT,
			ErrorCode::Invalid => StatusCode::BAD_REQUEST,
			ErrorCode::Unsupported => StatusCode::NOT_IMPLEMENTED,
			ErrorCode::Unauthorized => StatusCode::UNAUTHORIZED,
			ErrorCode::Forbidden => StatusCode::FORBIDDEN,
			ErrorCode::Engine => StatusCode::SERVICE_UNAVAILABLE,
		};
		let mut err = Self::new(status, value.code.as_str(), value.message);
		if matches!(value.code, ErrorCode::Unauthorized) {
			err.authenticate = true;
		}
		err
	}
}

impl IntoResponse for ApiError {
	fn into_response(self) -> Response {
		let mut response = (self.status, Json(self.body)).into_response();
		if self.authenticate {
			response
				.headers_mut()
				.insert(header::WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
		}
		response
	}
}

/// Convert a failed blocking task into a retryable engine error.
pub fn join_error(err: tokio::task::JoinError) -> ApiError {
	ApiError::new(
		StatusCode::SERVICE_UNAVAILABLE,
		"engine_error",
		format!("engine task failed: {err}"),
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn error_body_carries_recovery_contract() {
		let invalid = ApiError::invalid("bad input");
		let body = serde_json::to_value(&invalid.body).expect("serialize error body");
		assert_eq!(body["code"], "invalid");
		assert_eq!(body["message"], "bad input");
		assert_eq!(body["retryable"], false);
		assert_eq!(body["action"], "correct the request parameters");

		let engine = ApiError::from(EngineError::engine("host unavailable"));
		assert!(engine.retryable());
	}
}
