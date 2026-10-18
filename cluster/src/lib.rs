//! Vibemon cluster layer: SWIM-style gossip memberlist plus a raft-replicated
//! KV store, combined behind the [`Cluster`] facade (ARCHITECTURE.md §5).

pub mod addrs;
pub mod gossip;
pub mod kv;
pub mod meta;

use std::{
	convert::Infallible,
	future::Future,
	net::{IpAddr, SocketAddr},
	path::PathBuf,
	sync::Arc,
	time::{Duration, Instant},
};

use anyhow::Context;
use axum::{
	Router,
	extract::Request,
	http::{HeaderValue, StatusCode, header},
	middleware::Next,
	response::{IntoResponse, Response},
	routing::Route,
};
pub use meta::{MemberMeta, NodeCapacity, NodeId, RaftStatus, Roles};
use openraft::async_runtime::WatchReceiver;
use parking_lot::Mutex;
use tokio::{
	net::TcpListener,
	sync::{Notify, watch},
	task::JoinHandle,
};
use tower::{Layer, Service};

use crate::{
	gossip::{Gossip, GossipConfig, Member},
	kv::{Kv, RaftKv, Reconciler},
};

/// Minimum spacing between gossip metadata publications.
const META_MIN_INTERVAL: Duration = Duration::from_secs(1);
/// Period at which the leader watch re-evaluates leadership confirmation.
const LEADER_RECHECK: Duration = Duration::from_millis(250);
/// Retry period for seeds while this process knows no peer.
const JOIN_RETRY: Duration = Duration::from_secs(2);

/// Settings for [`Cluster::start`], resolved by `vmon serve`.
#[derive(Clone, Debug)]
pub struct ClusterConfig {
	pub id:               NodeId,
	/// Data directory; raft state lives in `<data_dir>/raft/`.
	pub data_dir:         PathBuf,
	pub roles:            Roles,
	pub gossip_bind:      SocketAddr,
	/// IPs peers may reach this process at, gossip and cluster listener alike,
	/// in preference order (at most 16). `None`: the gossip bind IP when
	/// specific, else every [`addrs::advertisable`] address, re-read every
	/// 30 s and re-advertised when it changes.
	pub advertise:        Option<Vec<IpAddr>>,
	/// Port of the internal HTTP listener serving [`Cluster::router`].
	pub cluster_port:     u16,
	/// Gossip seeds (`host:port`).
	pub join:             Vec<String>,
	/// Shared cluster secret (HTTP bearer, gossip HMAC key material).
	pub secret:           Vec<u8>,
	/// Drivers required before the first raft bootstrap (§5.4).
	pub bootstrap_expect: usize,
	/// How long a voter may be Dead/Left in gossip before the leader removes it.
	pub raft_reap_after:  Duration,
}

/// Running cluster membership + KV; cheap to clone.
#[derive(Clone)]
pub struct Cluster {
	pub gossip: Gossip,
	pub kv:     Kv,
	inner:      Arc<Inner>,
}

struct Inner {
	id:         NodeId,
	secret:     Arc<[u8]>,
	meta:       MetaPublisher,
	leader:     watch::Receiver<Option<NodeId>>,
	/// Keeps the leader channel open on node-only processes.
	_leader_tx: Arc<watch::Sender<Option<NodeId>>>,
	stop:       watch::Sender<bool>,
	tasks:      Mutex<Vec<JoinHandle<()>>>,
}

impl Cluster {
	/// Starts gossip (joining seeds), raft on drivers, and the background
	/// reconcilers.
	pub async fn start(cfg: ClusterConfig, mut meta: MemberMeta) -> anyhow::Result<Self> {
		anyhow::ensure!(!cfg.secret.is_empty(), "cluster secret must not be empty");
		let secret: Arc<[u8]> = cfg.secret.clone().into();
		let auth = Arc::new(bearer(&secret)?);
		meta.roles = cfg.roles;
		meta.cluster_port = cfg.cluster_port;
		meta.raft = cfg.roles.driver.then(RaftStatus::default);

		// Empty: gossip derives the list from the bind address / interfaces.
		let advertise = cfg
			.advertise
			.iter()
			.flatten()
			.map(|ip| SocketAddr::new(*ip, 0))
			.collect();
		let gossip_cfg = GossipConfig::new(cfg.id, cfg.gossip_bind, advertise, cfg.secret.clone());
		let gossip = Gossip::start(gossip_cfg, meta.encode())
			.await
			.context("starting gossip")?;
		let http = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(2))
			.pool_idle_timeout(Duration::from_mins(1))
			.build()
			.context("building cluster http client")?;

		let local = if cfg.roles.driver {
			let dir = cfg.data_dir.join("raft");
			Some(
				RaftKv::start(cfg.id, &dir, gossip.clone(), http.clone(), auth.clone())
					.await
					.context("starting raft")?,
			)
		} else {
			None
		};
		let kv = Kv::new(cfg.id, gossip.clone(), kv::KvClient::new(http, auth), local.clone());

		let (stop, stop_rx) = watch::channel(false);
		let meta = MetaPublisher::new(meta, gossip.clone());
		let (leader_tx, leader) = watch::channel(None);
		let leader_tx = Arc::new(leader_tx);
		let mut tasks = vec![tokio::spawn(meta.clone().run(stop_rx.clone()))];

		if !cfg.join.is_empty() {
			let joined = match gossip.join(&cfg.join).await {
				Ok(contacted) => contacted > 0,
				Err(error) => {
					tracing::warn!(%error, seeds = ?cfg.join, "gossip join failed; retrying in background");
					false
				},
			};
			if !joined {
				tasks.push(tokio::spawn(rejoin(gossip.clone(), cfg.join.clone(), stop_rx.clone())));
			}
		}

		if let Some(local) = local {
			tasks.push(tokio::spawn(track_leader(local.clone(), leader_tx.clone(), stop_rx.clone())));
			tasks.push(tokio::spawn(kv::maintain_journal(local.clone(), stop_rx.clone())));
			let reconciler = Reconciler::new(
				cfg.id,
				local,
				gossip.clone(),
				meta.clone(),
				cfg.bootstrap_expect,
				cfg.raft_reap_after,
				format!("http://{}", SocketAddr::new(gossip.local_addr().ip(), cfg.cluster_port)),
			);
			tasks.push(tokio::spawn(reconciler.run(stop_rx)));
		}

		let inner = Inner {
			id: cfg.id,
			secret,
			meta,
			leader,
			_leader_tx: leader_tx,
			stop,
			tasks: Mutex::new(tasks),
		};
		Ok(Self { gossip, kv, inner: Arc::new(inner) })
	}

	/// `/cluster/v1/*` (raft + KV) with bearer auth; empty on node-only
	/// processes.
	pub fn router(&self) -> Router {
		match self.kv.local() {
			Some(local) => kv::routes(local.clone()).layer(require_secret(self.inner.secret.clone())),
			None => Router::new(),
		}
	}

	/// Serves the KV over the Redis protocol (`AUTH <cluster secret>`) on
	/// `listener` until `shutdown` resolves; `vmon serve --kv-resp-listen`.
	pub async fn serve_resp(&self, listener: TcpListener, shutdown: impl Future<Output = ()>) {
		kv::serve_resp(listener, self.kv.clone(), self.inner.secret.clone(), shutdown).await;
	}

	/// This process's id.
	pub fn local_id(&self) -> NodeId {
		self.inner.id
	}

	/// Edits published metadata; publications are coalesced and spaced ≥ 1 s
	/// apart.
	pub fn update_meta(&self, f: impl FnOnce(&mut MemberMeta)) {
		self.inner.meta.update(f);
	}

	/// All gossip members (self included) whose metadata decodes.
	pub fn members(&self) -> Vec<(Member, MemberMeta)> {
		self
			.gossip
			.members()
			.into_iter()
			.filter_map(|m| MemberMeta::decode(&m.meta).map(|meta| (m, meta)))
			.collect()
	}

	/// Cluster listener URL of member `id` through the address this process
	/// currently reaches it at; `None` when gossip does not know the member or
	/// its metadata. Liveness is the caller's concern.
	pub fn cluster_url(&self, id: NodeId) -> Option<String> {
		kv::member_url(&self.gossip, id)
	}

	/// Current raft leader as seen locally (always `None` on node-only
	/// processes).
	///
	/// This node appears here only once its leadership is confirmed for the
	/// current term and recently acknowledged by a quorum, and is dropped as
	/// soon as it steps down; leader-only work should key off this watch.
	pub fn leader(&self) -> watch::Receiver<Option<NodeId>> {
		self.inner.leader.clone()
	}

	/// Whether this process is the raft leader with confirmed authority (same
	/// rule as [`Cluster::leader`]).
	pub fn is_leader(&self) -> bool {
		self.kv.local().is_some_and(|local| local.is_leader())
	}

	/// Stops background work and raft, then leaves gossip gracefully.
	pub async fn shutdown(&self) {
		if self.inner.stop.send_replace(true) {
			return;
		}
		let tasks = std::mem::take(&mut *self.inner.tasks.lock());
		for task in tasks {
			task.abort();
		}
		if let Some(local) = self.kv.local()
			&& let Err(error) = local.raft().shutdown().await
		{
			tracing::warn!(%error, "raft shutdown failed");
		}
		self.gossip.leave(Duration::from_secs(2)).await;
	}
}

/// Mirrors the local raft leader into the facade's watch channel.
async fn track_leader(
	local: Arc<RaftKv>,
	tx: Arc<watch::Sender<Option<NodeId>>>,
	mut stop: watch::Receiver<bool>,
) {
	let mut metrics = local.raft().metrics();
	// Confirmation also lapses with time (stale quorum ack), so re-check
	// periodically.
	let mut recheck = tokio::time::interval(LEADER_RECHECK);
	recheck.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
	loop {
		metrics.borrow_and_update();
		let leader = local.reported_leader();
		tx.send_if_modified(|current| std::mem::replace(current, leader) != leader);
		tokio::select! {
			changed = metrics.changed() => if changed.is_err() { return },
			_ = recheck.tick() => {},
			_ = stop.changed() => return,
		}
	}
}

/// Retries the seeds until one answers.
async fn rejoin(gossip: Gossip, seeds: Vec<String>, mut stop: watch::Receiver<bool>) {
	loop {
		tokio::select! {
			() = tokio::time::sleep(JOIN_RETRY) => {},
			_ = stop.changed() => return,
		}
		if matches!(gossip.join(&seeds).await, Ok(contacted) if contacted > 0) {
			return;
		}
	}
}

/// Coalescing, rate-limited publisher of this member's gossip metadata.
#[derive(Clone)]
pub(crate) struct MetaPublisher {
	shared: Arc<MetaShared>,
}

struct MetaShared {
	meta:   Mutex<MemberMeta>,
	dirty:  Notify,
	gossip: Gossip,
}

impl MetaPublisher {
	fn new(meta: MemberMeta, gossip: Gossip) -> Self {
		Self { shared: Arc::new(MetaShared { meta: Mutex::new(meta), dirty: Notify::new(), gossip }) }
	}

	/// Applies `f`; schedules a publication only if the metadata changed.
	pub(crate) fn update(&self, f: impl FnOnce(&mut MemberMeta)) {
		let mut meta = self.shared.meta.lock();
		let before = meta.clone();
		f(&mut meta);
		if *meta != before {
			self.shared.dirty.notify_one();
		}
	}

	/// Publishes the latest metadata after each change, at most once per
	/// [`META_MIN_INTERVAL`].
	async fn run(self, mut stop: watch::Receiver<bool>) {
		let mut last: Option<Instant> = None;
		loop {
			tokio::select! {
				() = self.shared.dirty.notified() => {},
				_ = stop.changed() => return,
			}
			if let Some(at) = last {
				tokio::time::sleep_until((at + META_MIN_INTERVAL).into()).await;
			}
			let bytes = self.shared.meta.lock().encode();
			self.shared.gossip.set_meta(bytes);
			last = Some(Instant::now());
		}
	}
}

pub(crate) fn bearer(secret: &[u8]) -> anyhow::Result<HeaderValue> {
	let mut value = b"Bearer ".to_vec();
	value.extend_from_slice(secret);
	let mut header =
		HeaderValue::from_bytes(&value).context("cluster secret is not a valid header value")?;
	header.set_sensitive(true);
	Ok(header)
}

/// Constant-time equality (length is not secret).
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
	if a.len() != b.len() {
		return false;
	}
	let diff = a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y));
	std::hint::black_box(diff) == 0
}

/// Whether `Authorization` carries `Bearer <secret>`; an empty secret matches
/// nothing.
fn authorized(value: Option<&HeaderValue>, secret: &[u8]) -> bool {
	let token = value.and_then(|v| v.as_bytes().strip_prefix(b"Bearer "));
	!secret.is_empty() && token.is_some_and(|token| constant_time_eq(token, secret))
}

/// Middleware layer requiring `Authorization: Bearer <cluster secret>` (401
/// otherwise).
///
/// Applied by [`Cluster::router`] and by `vmon-node` to `/node/v1`:
/// `router.layer(require_secret(secret))`.
pub fn require_secret(
	secret: Arc<[u8]>,
) -> impl Layer<
	Route,
	Service: Service<Request, Response = Response, Error = Infallible, Future: Send + 'static>
	            + Clone
	            + Send
	            + Sync
	            + 'static,
> + Clone
+ Send
+ Sync
+ 'static {
	axum::middleware::from_fn(move |request: Request, next: Next| {
		let ok = authorized(request.headers().get(header::AUTHORIZATION), &secret);
		async move {
			if ok {
				next.run(request).await
			} else {
				(StatusCode::UNAUTHORIZED, "invalid cluster secret").into_response()
			}
		}
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn bearer_check_requires_exact_secret() {
		let secret = b"s3cret";
		let header = |v: &str| HeaderValue::from_str(v).unwrap();
		assert!(authorized(Some(&header("Bearer s3cret")), secret));
		assert!(!authorized(Some(&header("Bearer s3cre")), secret));
		assert!(!authorized(Some(&header("Bearer s3cretx")), secret));
		assert!(!authorized(Some(&header("bearer s3cret")), secret));
		assert!(!authorized(Some(&header("s3cret")), secret));
		assert!(!authorized(None, secret));
		assert!(!authorized(Some(&header("Bearer ")), b""));
	}
}
