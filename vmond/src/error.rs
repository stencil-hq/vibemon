//! Server-wide error envelope.
//!
//! Every engine failure carries a stable `code` the API layer maps onto HTTP
//! statuses and the `{"code","message"}` wire envelope. Codes mirror
//! `python/vmon/core.py`'s exception taxonomy.

use std::fmt;

/// Stable error codes shared across the engine and the v1 API.
///
/// HTTP mapping (see the API layer): `not_found→404`, `not_running→409`,
/// `busy→409`, `invalid→400`, `unsupported→501`, `unauthorized→401`,
/// `forbidden→403`, and everything else `→503`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ErrorCode {
	NotFound,
	NotRunning,
	Busy,
	Invalid,
	Unsupported,
	Unauthorized,
	Forbidden,
	/// Unclassified engine failure (maps to 503 like Python's bare
	/// `EngineError`).
	Engine,
}

impl ErrorCode {
	/// The wire spelling used in the JSON envelope (Python `code` fields).
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::NotFound => "not_found",
			Self::NotRunning => "not_running",
			Self::Busy => "busy",
			Self::Invalid => "invalid",
			Self::Unsupported => "unsupported",
			Self::Unauthorized => "unauthorized",
			Self::Forbidden => "forbidden",
			Self::Engine => "engine_error",
		}
	}
}

/// An engine failure with a stable code and a human-readable message.
#[derive(Clone, Debug)]
pub struct EngineError {
	pub code:    ErrorCode,
	pub message: String,
}

impl EngineError {
	pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
		Self { code, message: message.into() }
	}

	pub fn not_found(message: impl Into<String>) -> Self {
		Self::new(ErrorCode::NotFound, message)
	}

	pub fn not_running(message: impl Into<String>) -> Self {
		Self::new(ErrorCode::NotRunning, message)
	}

	pub fn busy(message: impl Into<String>) -> Self {
		Self::new(ErrorCode::Busy, message)
	}

	pub fn invalid(message: impl Into<String>) -> Self {
		Self::new(ErrorCode::Invalid, message)
	}

	pub fn unsupported(message: impl Into<String>) -> Self {
		Self::new(ErrorCode::Unsupported, message)
	}

	pub fn unauthorized(message: impl Into<String>) -> Self {
		Self::new(ErrorCode::Unauthorized, message)
	}

	pub fn forbidden(message: impl Into<String>) -> Self {
		Self::new(ErrorCode::Forbidden, message)
	}

	pub fn engine(message: impl Into<String>) -> Self {
		Self::new(ErrorCode::Engine, message)
	}
}

impl fmt::Display for EngineError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		write!(f, "{}", self.message)
	}
}

impl std::error::Error for EngineError {}

impl From<std::io::Error> for EngineError {
	fn from(e: std::io::Error) -> Self {
		Self::engine(e.to_string())
	}
}

impl From<serde_json::Error> for EngineError {
	fn from(e: serde_json::Error) -> Self {
		Self::engine(e.to_string())
	}
}

/// Crate-wide result; the error defaults to [`EngineError`].
pub type Result<T, E = EngineError> = std::result::Result<T, E>;
