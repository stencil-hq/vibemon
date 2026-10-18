//! Vibemon driver role: the E2B-compatible control plane (ARCHITECTURE.md §9).
//!
//! A driver serves the public E2B REST API and sandbox ingress, keeps durable
//! state ([`store`]) and coordinates live state in the raft KV of
//! [`vmon_cluster`], and drives nodes through [`vmon_node::client`].

pub mod admin;
pub mod api;
pub mod auth;
pub mod builds;
pub mod crypto;
pub mod dashboard;
pub mod error;
pub mod ingress;
pub mod lifecycle;
pub mod scheduler;
pub mod state;
pub mod stats;
pub mod store;
pub mod webhooks;

#[cfg(test)]
mod db_tests;

use vmon_cluster::Cluster;

pub use crate::state::{AppState, DriverConfig, TeamLimits};

/// A running driver: shared state plus its background tasks.
pub struct Driver {
	state: AppState,
}

impl Driver {
	/// Opens the store on the cluster KV and starts background work (reaper,
	/// reconciler, build supervisor, spawn counts, webhook delivery, fleet
	/// sampler). Must run inside a Tokio runtime.
	pub fn start(config: DriverConfig, cluster: Cluster) -> Self {
		let store = store::Store::new(cluster.kv.clone());
		let keys = crypto::Keys::derive(config.cluster_secret.as_bytes());
		let state = AppState::new(config, store, cluster, keys);
		lifecycle::spawn_background(state.clone());
		builds::spawn_supervisor(state.clone());
		builds::spawn_spawn_writer(state.clone());
		webhooks::spawn_delivery(state.clone());
		stats::spawn_sampler(state.clone());
		Self { state }
	}

	/// Public router: sandbox ingress (by Host / `E2b-Sandbox-*` headers) in
	/// front of the E2B API and the admin dashboard.
	pub fn public_router(&self) -> axum::Router {
		let api = api::router(self.state.clone()).merge(dashboard::router());
		ingress::wrap(self.state.clone(), api)
	}

	/// Writes state still buffered in memory (template spawn counts) before the
	/// process exits; bounded so a stalled KV cannot hang shutdown.
	pub async fn shutdown(&self) {
		const FLUSH_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(10);
		let flushed = tokio::time::timeout(FLUSH_TIMEOUT, builds::flush_spawns(&self.state)).await;
		if flushed != Ok(true) {
			tracing::warn!("some template spawn counts were not written");
		}
	}

	/// Shared state (used by the `vmon` binary for admin wiring and tests).
	pub const fn state(&self) -> &AppState {
		&self.state
	}
}
