//! Node-local GitHub Actions cache for CI sandboxes (ARCHITECTURE.md §9.7).
//!
//! The `ci` template's runner exports `ACTIONS_RESULTS_URL=http://<guest
//! gateway>:PORT/` (the node side of the sandbox's TAP). The egress proxy
//! hands every guest connection to `<lease host ip>:PORT` to [`Service`]
//! instead of dialing it, together with the sandbox the TAP lease belongs to;
//! nothing the guest sends identifies it. The service speaks GitHub's cache
//! service v2 (Twirp `github.actions.results.api.v1.CacheService` with JSON
//! bodies, plus the Azure Blob calls `@actions/cache` and buildx make against
//! the signed URLs it returns), serves only sandboxes the driver granted a
//! CI scope ([`records::CacheGrant`]), and forwards every other Twirp call
//! (artifacts, …) unchanged to GitHub's results service.
//!
//! Archives live on the node's disk under `<data-dir>/ci-cache/`
//! ([`blobs`]); the index lives in the raft KV ([`index`]). A restore on a
//! node without the archive fetches it from a holder over the cluster
//! listener (`GET /node/v1/ci-cache/blobs/{digest}`) while streaming it to the
//! guest.

pub mod blobs;
mod http;
pub mod index;
pub mod records;
#[cfg(test)]
mod tests;
mod upload;

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use chrono::{DateTime, SubsecRound as _, Utc};
use url::Url;
use vmon_cluster::Cluster;

pub use self::http::{Listener, Service, routes};

/// Port of the cache service on each TAP's host address.
pub const PORT: u16 = 7948;
/// Path the runner start command probes before pointing the runner at the
/// node (`200 ok` from any sandbox).
pub const PING_PATH: &str = "/_vmon/ping";
/// GitHub's results service (Twirp calls other than the cache go there).
pub const DEFAULT_UPSTREAM: &str = "https://results-receiver.actions.githubusercontent.com/";
/// How often each node drops archives no entry uses and expired uploads.
const GC_INTERVAL: Duration = Duration::from_mins(10);

/// Node cache settings (`vmon serve` flags).
#[derive(Clone, Debug)]
pub struct CacheConfig {
	/// `<data-dir>/ci-cache`.
	pub dir:      PathBuf,
	/// Disk the local archives may occupy, in bytes (`--ci-cache-mb`).
	pub budget:   u64,
	/// GitHub's results service for pass-through calls
	/// (`--ci-results-upstream`).
	pub upstream: Url,
	/// Cluster secret (peer archive fetches).
	pub secret:   String,
	pub http:     reqwest::Client,
}

/// Starts the service and its periodic cleanup.
pub fn start(config: CacheConfig, cluster: Cluster) -> anyhow::Result<Arc<Service>> {
	let service = Service::new(config, cluster)?;
	let weak = Arc::downgrade(&service);
	tokio::spawn(async move {
		let mut ticker = tokio::time::interval(GC_INTERVAL);
		ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		loop {
			ticker.tick().await;
			let Some(service) = weak.upgrade() else {
				return;
			};
			service.gc().await;
		}
	});
	Ok(service)
}

/// Store timestamps (millisecond precision, like the driver store's).
pub(crate) fn now() -> DateTime<Utc> {
	Utc::now().trunc_subsecs(3)
}

/// A guest connection to `<lease host ip>:PORT` handed over by the egress
/// proxy.
pub trait LocalService: Send + Sync {
	/// Serves `stream` from sandbox `sandbox_id`, which dialed `local`.
	fn serve(&self, stream: tokio::net::TcpStream, sandbox_id: String, local: SocketAddr);
}
