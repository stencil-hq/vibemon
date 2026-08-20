//! Short-lived workload identity shared by registry, object-store, and VMM
//! clients.
//!
//! Provider metadata credentials are resolved on demand and cached only until
//! their refresh window. Secrets never need to be persisted or placed in child
//! process arguments.

use std::fmt;

use serde::{Deserialize, Serialize};

pub mod aws;
pub mod google;

/// Authentication scheme used for a range-addressable cloud object.
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ObjectAuth {
	/// No provider authentication; suitable for public or pre-signed URLs.
	#[default]
	None,
	/// Google OAuth token minted from the workload metadata server.
	Google,
	/// AWS Signature Version 4 using workload credentials.
	Aws,
}

impl ObjectAuth {
	/// Stable command-line spelling passed between the daemon and VMM.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::None => "none",
			Self::Google => "google",
			Self::Aws => "aws",
		}
	}
}

impl std::str::FromStr for ObjectAuth {
	type Err = Error;

	fn from_str(value: &str) -> Result<Self> {
		match value {
			"none" => Ok(Self::None),
			"google" => Ok(Self::Google),
			"aws" => Ok(Self::Aws),
			_ => Err(Error::new(format!("unsupported cloud object auth {value:?}"))),
		}
	}
}

/// Failure while resolving workload identity or signing a cloud request.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
	message: String,
}

impl Error {
	/// Build an error with a stable human-readable message.
	pub fn new(message: impl Into<String>) -> Self {
		Self { message: message.into() }
	}
}

impl fmt::Display for Error {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(&self.message)
	}
}

impl std::error::Error for Error {}

/// Result returned by cloud identity operations.
pub type Result<T> = std::result::Result<T, Error>;
