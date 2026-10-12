//! Generated envd message types (prost) with canonical proto3 JSON (pbjson),
//! built by `agent/build.rs` from the vendored `process.proto` and
//! `filesystem.proto`. Used by the Connect codec and the service handlers.

/// `package process;` messages.
#[allow(
	clippy::all,
	clippy::pedantic,
	clippy::nursery,
	clippy::allow_attributes_without_reason,
	missing_docs,
	reason = "generated code"
)]
pub mod process {
	include!(concat!(env!("OUT_DIR"), "/process.rs"));
	include!(concat!(env!("OUT_DIR"), "/process.serde.rs"));
}

/// `package filesystem;` messages.
#[allow(
	clippy::all,
	clippy::pedantic,
	clippy::nursery,
	clippy::allow_attributes_without_reason,
	missing_docs,
	reason = "generated code"
)]
pub mod filesystem {
	include!(concat!(env!("OUT_DIR"), "/filesystem.rs"));
	include!(concat!(env!("OUT_DIR"), "/filesystem.serde.rs"));
}
