//! Template builds on the driver side (ARCHITECTURE.md §8.6, §9.4).
//!
//! Registering and starting builds on a builder node, supervising them to
//! completion, resolving template references for sandbox creation, file
//! context uploads, snapshot templates, and artifact bookkeeping.
//!
//! Entry points: [`spawn_supervisor`] and [`spawn_spawn_writer`] (called by
//! [`crate::Driver::start`]), [`resolve_template`] and the snapshot helpers
//! (called by the sandbox API), and the build operations used by
//! [`crate::api::templates`].

mod artifacts;
mod files;
pub mod names;
mod resolve;
mod snapshots;
mod spawns;
mod start;
mod supervisor;
#[cfg(test)]
mod tests;

use std::time::Duration;

use vmon_cluster::NodeId;
use vmon_node::api::ArtifactInfo;

#[cfg(test)]
pub(crate) use self::supervisor::housekeeping;
pub use self::{
	artifacts::{artifact_sources, published, record_artifact, record_holder},
	files::{
		ArchiveKind, FileRecord, file_present, sniff_archive, store_upload, upload_path, upload_url,
	},
	resolve::{
		Lookup, TemplateAccess, find_template, lookup_order, owned_template, resolve_template,
	},
	snapshots::{
		SnapshotInfo, SnapshotKind, SnapshotRequest, SnapshotTarget, begin_snapshot, fail_snapshot,
		finish_snapshot, list_snapshots, retire_fork,
	},
	spawns::{SpawnCounter, flush_spawns, spawn_spawn_writer},
	start::{
		BuildRequest, RegisteredBuild, StartRequest, cancel_build, delete_template, register_build,
		start_build,
	},
	supervisor::spawn_supervisor,
};

/// Maximum wall time of a build (upstream `buildTimeout`).
pub const BUILD_TIMEOUT: Duration = Duration::from_hours(1);
/// Upstream failure message when [`BUILD_TIMEOUT`] elapses.
pub const BUILD_TIMEOUT_MESSAGE: &str =
	"build status polling timed out. Maximum build time is 1h0m0s.";

/// An alive node holding a build's artifacts. Fetching nodes resolve the id
/// to a route of their own (`Cluster::cluster_url`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactSource {
	pub node_id: NodeId,
	pub info:    ArtifactInfo,
}

/// A template reference resolved to a concrete ready build.
#[derive(Clone, Debug)]
pub struct ResolvedTemplate {
	pub template_id: String,
	pub build_id:    String,
	/// The alias used in the reference, else the template's first alias.
	pub alias:       Option<String>,
	/// Build facts recorded when the build became ready (resources,
	/// envd version, envd defaults).
	pub artifact:    ArtifactInfo,
	/// Alive nodes holding the artifacts (possibly empty).
	pub sources:     Vec<ArtifactSource>,
}

/// KV key of an artifact location record.
pub fn art_key(build_id: &str, node_id: NodeId) -> String {
	format!("art/{build_id}/{node_id}")
}

/// KV prefix of a build's artifact location records.
pub fn art_prefix(build_id: &str) -> String {
	format!("art/{build_id}/")
}

/// KV key of an uploaded file-context archive.
pub fn file_key(template_id: &str, hash: &str) -> String {
	format!("file/{template_id}/{hash}")
}

/// KV key of a build's supervision lease.
pub fn lock_key(build_id: &str) -> String {
	format!("lock/build/{build_id}")
}
