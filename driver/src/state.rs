//! Shared driver state handed to every handler and background task.

use std::{ops::Deref, path::PathBuf, sync::Arc, time::Duration};

use vmon_cluster::{Cluster, MemberMeta, NodeId, gossip::MemberState};
use vmon_node::client::NodeClient;

use crate::{
	auth::AuthCache, builds::SpawnCounter, crypto::Keys, error::ApiError, scheduler::Scheduler,
	stats::FleetStats, store::Store,
};

/// Driver configuration resolved by the `vmon serve` front end.
#[derive(Clone, Debug)]
pub struct DriverConfig {
	/// Sandbox domain (`<port>-<id>.<domain>`); the API lives at `api.<domain>`.
	pub domain:         String,
	/// Public base URL of this driver (used in signed upload URLs).
	pub public_url:     String,
	/// Value accepted in `X-Admin-Token`; `None` disables admin auth.
	pub admin_token:    Option<String>,
	/// Shared cluster secret (bearer for node calls, HKDF input for [`Keys`]).
	pub cluster_secret: String,
	/// Directory for driver-local files (build file contexts awaiting a
	/// builder).
	pub data_dir:       PathBuf,
	/// Limits applied to teams created without explicit limits.
	pub default_limits: TeamLimits,
	/// Volumes live on storage every node sees (`--shared-volumes`):
	/// `volumes.node_id` stays NULL and any node can serve them.
	pub shared_volumes: bool,
	/// Operator object store (`--object-store`): removes portable paused
	/// state on kill.
	pub object_store:   Option<vmon_cloud::s3::S3Client>,
}

/// Per-team resource limits (E2B tier limits), stored on the `teams` row.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TeamLimits {
	/// Concurrent running sandboxes (429 beyond).
	pub max_concurrent_sandboxes: u32,
	/// Concurrent template builds (429 beyond).
	pub max_concurrent_builds:    u32,
	/// Per-sandbox vCPU ceiling.
	pub max_vcpus:                u32,
	/// Per-sandbox memory ceiling.
	pub max_memory_mb:            u64,
	/// Per-sandbox disk ceiling.
	pub max_disk_mb:              u64,
	/// Maximum sandbox lifetime per execution.
	pub max_length_hours:         u32,
}

impl Default for TeamLimits {
	fn default() -> Self {
		Self {
			max_concurrent_sandboxes: 100,
			max_concurrent_builds:    20,
			max_vcpus:                8,
			max_memory_mb:            8192,
			max_disk_mb:              20480,
			max_length_hours:         24,
		}
	}
}

/// Cheaply cloneable handle to everything a handler needs.
#[derive(Clone)]
pub struct AppState(Arc<Inner>);

/// Shared contents of [`AppState`].
pub struct Inner {
	pub config:     DriverConfig,
	pub store:      Store,
	pub cluster:    Cluster,
	pub keys:       Keys,
	/// Shared outbound HTTP pool (node calls, webhooks).
	pub http:       reqwest::Client,
	/// Short-lived API-key → team cache used by [`crate::auth`].
	pub auth_cache: AuthCache,
	/// Driver-local placement counters (inflight creates, per-node outcomes).
	pub scheduler:  Scheduler,
	/// Recent fleet samples ([`crate::stats`]).
	pub stats:      FleetStats,
	/// Template spawn counts awaiting their background write.
	pub spawns:     SpawnCounter,
}

impl Deref for AppState {
	type Target = Inner;

	fn deref(&self) -> &Inner {
		&self.0
	}
}

impl AppState {
	/// Builds the shared state; called by [`crate::Driver::start`].
	pub fn new(config: DriverConfig, store: Store, cluster: Cluster, keys: Keys) -> Self {
		let http = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(5))
			.pool_idle_timeout(Duration::from_secs(90))
			.build()
			.expect("reqwest client builds");
		Self(Arc::new(Inner {
			config,
			store,
			cluster,
			keys,
			http,
			auth_cache: AuthCache::default(),
			scheduler: Scheduler::default(),
			stats: FleetStats::default(),
			spawns: SpawnCounter::default(),
		}))
	}

	/// Alive members serving the node role, with decoded metadata.
	pub fn node_members(&self) -> Vec<(NodeId, MemberMeta)> {
		self
			.cluster
			.members()
			.into_iter()
			.filter(|(member, meta)| member.state == MemberState::Alive && meta.roles.node)
			.map(|(member, meta)| (member.id, meta))
			.collect()
	}

	/// Client for a node by id; fails when the node is not an alive member.
	pub fn node(&self, id: NodeId) -> Result<NodeClient, ApiError> {
		let (_, meta) = self
			.cluster
			.members()
			.into_iter()
			.find(|(member, meta)| {
				member.id == id && member.state != MemberState::Dead && meta.roles.node
			})
			.ok_or_else(|| ApiError::unavailable(format!("node {id} is not available")))?;
		self.node_at(&meta.cluster_url)
	}

	/// Client for a node by cluster URL.
	pub fn node_at(&self, cluster_url: &str) -> Result<NodeClient, ApiError> {
		NodeClient::new(cluster_url, &self.config.cluster_secret, self.http.clone())
			.map_err(|e| ApiError::internal(e.to_string()))
	}
}
