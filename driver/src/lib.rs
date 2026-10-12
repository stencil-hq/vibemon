//! Vibemon driver role: the E2B-compatible control plane (ARCHITECTURE.md §9).
//!
//! A driver serves the public E2B REST API and sandbox ingress, keeps durable
//! state in `MySQL` ([`store`]), coordinates live state through the raft KV of
//! [`vmon_cluster`], and drives nodes through [`vmon_node::client`].

pub mod admin;
pub mod api;
pub mod auth;
pub mod builds;
pub mod crypto;
pub mod error;
pub mod ingress;
pub mod lifecycle;
pub mod scheduler;
pub mod state;
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
	/// Connects to `MySQL`, applies migrations, and starts background work
	/// (reaper, reconciler, build supervisor, webhook delivery).
	pub async fn start(
		config: DriverConfig,
		mysql_url: &str,
		cluster: Cluster,
	) -> anyhow::Result<Self> {
		let store = store::Store::connect(mysql_url).await?;
		store.migrate().await?;
		let keys = crypto::Keys::derive(config.cluster_secret.as_bytes());
		let state = AppState::new(config, store, cluster, keys);
		lifecycle::spawn_background(state.clone());
		builds::spawn_supervisor(state.clone());
		webhooks::spawn_delivery(state.clone());
		Ok(Self { state })
	}

	/// Public router: sandbox ingress (by Host / `E2b-Sandbox-*` headers) in
	/// front of the E2B API.
	pub fn public_router(&self) -> axum::Router {
		ingress::wrap(self.state.clone(), api::router(self.state.clone()))
	}

	/// Shared state (used by the `vmon` binary for admin wiring and tests).
	pub const fn state(&self) -> &AppState {
		&self.state
	}
}
