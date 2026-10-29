//! Raft KV records of the CI cache (ARCHITECTURE.md §9.7), shared by the
//! nodes (which serve the cache) and the driver (which grants scopes, sweeps,
//! and lists).
//!
//! Keys (store rules: serde JSON under `db/<table>/…`, segments escaped):
//! - `db/ci_cache/<repo id>/<scope>/<key>/<version>` → [`CacheEntryRow`]: one
//!   cache entry. `Uploading` rows carry a 1 h TTL (an abandoned upload
//!   expires); `Ready` rows live until the driver's sweep removes them (7 days
//!   unused, or the repository's budget).
//! - `db/ci_cache_blob/<sha256>` → [`CacheBlobRow`]: a content-addressed
//!   archive, the nodes holding a copy, and the entries using it; deleted with
//!   its last entry or its last holder.
//! - `db/ci_cache_grant/<sandbox id>` → [`CacheGrant`] (TTL = the CI sandbox
//!   timeout): the driver's statement that a sandbox is a CI runner, and once
//!   GitHub reported the job it runs, that job's cache scope.

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vmon_cluster::NodeId;

/// Entry table.
pub const ENTRIES: &str = "db/ci_cache";
/// Blob table.
pub const BLOBS: &str = "db/ci_cache_blob";
/// Grant table.
pub const GRANTS: &str = "db/ci_cache_grant";

/// What one job may read and write, resolved by the driver from the job's
/// workflow run (never from anything the job sends).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheScope {
	/// GitHub's repository id (stable across renames).
	pub repo_id: u64,
	/// `owner/name` when the scope was resolved (display only).
	pub repo:    String,
	/// The ref the job writes to: `refs/heads/<branch>`,
	/// `refs/pull/<n>/merge`, `refs/tags/<tag>`, or an isolated
	/// `refs/vmon/runs/<run id>` when the run's ref could not be determined.
	pub write:   String,
	/// Refs searched on restore, in order; starts with `write`, then the pull
	/// request's base branch, then the default branch.
	pub read:    Vec<String>,
}

/// A CI sandbox's cache permission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheGrant {
	/// The vmon runner record the sandbox belongs to.
	pub runner: String,
	/// The job GitHub reported running on the runner (`None` until then).
	#[serde(default)]
	pub job_id: Option<u64>,
	/// The job's scope (`None` until resolved).
	#[serde(default)]
	pub scope:  Option<CacheScope>,
}

/// Lifecycle of an entry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum EntryState {
	/// Reserved by `CreateCacheEntry`; the archive is being uploaded.
	Uploading,
	/// Finalized: restorable.
	Ready,
}

/// One cache entry `(repository, scope, key, version)`; immutable once
/// ready.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheEntryRow {
	/// `entry_id` reported to the toolkit.
	pub id:           u64,
	pub repo_id:      u64,
	pub repo:         String,
	pub scope:        String,
	pub key:          String,
	pub version:      String,
	pub state:        EntryState,
	/// sha256 (lowercase hex) of the archive once ready.
	#[serde(default)]
	pub digest:       Option<String>,
	#[serde(default)]
	pub size:         u64,
	/// Node that took the upload.
	pub node:         NodeId,
	/// Job that created the entry.
	pub job_id:       u64,
	pub created_at:   DateTime<Utc>,
	pub last_used_at: DateTime<Utc>,
}

/// One stored archive.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CacheBlobRow {
	pub size:       u64,
	/// Nodes with a local copy.
	pub holders:    Vec<NodeId>,
	/// KV keys of the entries using the archive.
	pub entries:    Vec<String>,
	pub created_at: DateTime<Utc>,
}

fn escape(segment: &str) -> String {
	segment.replace('%', "%25").replace('/', "%2F")
}

/// KV key of an entry.
pub fn entry_key(repo_id: u64, scope: &str, key: &str, version: &str) -> String {
	format!("{}/{}", key_prefix(repo_id, scope, key), escape(version))
}

/// Prefix of every entry of `scope` whose key starts with `key_prefix`
/// (escaping maps characters one by one, so prefixes stay prefixes).
pub fn key_prefix(repo_id: u64, scope: &str, key_prefix: &str) -> String {
	format!("{ENTRIES}/{repo_id}/{}/{}", escape(scope), escape(key_prefix))
}

/// Prefix of every entry of a repository.
pub fn repo_prefix(repo_id: u64) -> String {
	format!("{ENTRIES}/{repo_id}/")
}

/// KV key of a blob.
pub fn blob_key(digest: &str) -> String {
	format!("{BLOBS}/{digest}")
}

/// KV key of a sandbox's grant.
pub fn grant_key(sandbox_id: &str) -> String {
	format!("{GRANTS}/{}", escape(sandbox_id))
}

/// Whether `digest` is a lowercase hex sha256.
pub fn valid_digest(digest: &str) -> bool {
	digest.len() == 64
		&& digest
			.bytes()
			.all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn keys_escape_segments_and_keep_prefixes() {
		let key = entry_key(7, "refs/heads/main", "cargo-a/b%", "v1");
		assert_eq!(key, "db/ci_cache/7/refs%2Fheads%2Fmain/cargo-a%2Fb%25/v1");
		assert!(key.starts_with(&key_prefix(7, "refs/heads/main", "cargo-a/")));
		let scope = key_prefix(7, "refs/heads/main", "");
		assert!(scope.ends_with('/'));
		assert!(!entry_key(7, "refs/heads/main2", "k", "v").starts_with(&scope));
		assert!(valid_digest(&"ab".repeat(32)));
		assert!(!valid_digest(&"AB".repeat(32)));
		assert!(!valid_digest("../x"));
	}
}
