//! vmon CI: GitHub Actions jobs with `runs-on: vmon…` run on one fresh
//! sandbox each (ARCHITECTURE.md §9.7).
//!
//! A GitHub App delivers `workflow_job` webhooks to
//! `POST /ci/github/webhook` ([`webhook`]), which only records them
//! ([`crate::store::ci`]). The leader's [`controller`] decides each queued
//! job (labels, linked team, public-repository policy), provisions a runner
//! (sandbox → repository JIT config → `run.sh --jitconfig` through the node's
//! process route), and tears it down when the job completes, after a debug
//! hold, when idle, or at the runtime cap. [`api`] serves the admin JSON API
//! for the dashboard and `vmon admin ci`.

pub mod api;
mod controller;
pub mod gate;
pub mod github;
pub mod labels;
pub mod webhook;

#[cfg(test)]
mod tests;

use std::collections::{HashMap, HashSet};

use axum::{Router, middleware::from_extractor_with_state, routing::post};
use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use tokio::sync::Notify;

pub use self::controller::spawn_controller;
use crate::{auth::AdminAuth, state::AppState};

/// Installation tokens by `(app id, installation id)` with their expiry.
type TokenCache = HashMap<(u64, u64), (String, DateTime<Utc>)>;

/// Driver-local CI state ([`AppState::ci`]).
#[derive(Default)]
pub struct Ci {
	tokens:      Mutex<TokenCache>,
	/// Wakes the controller (a webhook was recorded).
	wake:        Notify,
	/// Keys of controller tasks in flight on this driver.
	inflight:    Mutex<HashSet<String>>,
	/// Last job polled by the reconcile (rotates through stale jobs).
	poll_cursor: Mutex<Option<u64>>,
	/// Last repository whose GitHub runners the reconcile checked.
	repo_cursor: Mutex<Option<(u64, String)>>,
}

impl Ci {
	/// Asks the controller for a pass now (on the leader).
	pub fn wake(&self) {
		self.wake.notify_one();
	}

	/// Forgets cached installation tokens (the app changed).
	pub fn forget_tokens(&self) {
		self.tokens.lock().clear();
	}
}

/// Public-listener routes: the webhook intake (HMAC-authenticated) and the
/// admin API under `/admin/ci` (`X-Admin-Token`).
pub fn routes(state: &AppState) -> Router<AppState> {
	Router::new()
		.route("/ci/github/webhook", post(webhook::receive))
		.nest(
			"/admin/ci",
			api::routes().route_layer(from_extractor_with_state::<AdminAuth, _>(state.clone())),
		)
}

/// Cluster-listener routes (`/driver/v1/ci`, cluster secret): the admin API
/// plus the webhook secret for the relay.
pub fn cluster_routes() -> Router<AppState> {
	Router::new().nest("/driver/v1/ci", api::routes().merge(api::secret_routes()))
}
