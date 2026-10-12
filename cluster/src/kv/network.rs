//! Raft RPC client: openraft network over cluster HTTP (`/cluster/v1/raft/*`).

use std::{error::Error, sync::Arc, time::Duration};

use openraft::{
	error::{InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Unreachable},
	network::{Backoff, RPCOption, RaftNetwork, RaftNetworkFactory},
	raft::{
		AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
		VoteRequest, VoteResponse,
	},
};
use reqwest::header::HeaderValue;
use serde::{Serialize, de::DeserializeOwned};

use super::raft::{RaftPeer, TypeConfig};
use crate::{
	gossip::{Gossip, MemberState},
	meta::NodeId,
};

/// Retry delay after an unreachable error while gossip still sees the peer.
const BACKOFF_MIN: Duration = Duration::from_millis(500);
/// Longest retry delay for a peer gossip reports Dead/Left (openraft logs
/// every failed attempt, so this bounds the log rate per dead voter).
const BACKOFF_MAX: Duration = Duration::from_secs(30);

/// Next replication retry delay after `previous`: [`BACKOFF_MIN`] while the
/// peer is live in gossip, otherwise doubling up to [`BACKOFF_MAX`].
fn next_backoff(previous: Duration, peer_live: bool) -> Duration {
	if peer_live {
		BACKOFF_MIN
	} else {
		(previous * 2).clamp(BACKOFF_MIN * 2, BACKOFF_MAX)
	}
}

/// Creates per-peer raft clients sharing one HTTP pool.
pub struct Network {
	http:   reqwest::Client,
	auth:   Arc<HeaderValue>,
	gossip: Gossip,
}

impl Network {
	pub const fn new(http: reqwest::Client, auth: Arc<HeaderValue>, gossip: Gossip) -> Self {
		Self { http, auth, gossip }
	}
}

impl RaftNetworkFactory<TypeConfig> for Network {
	type Network = PeerClient;

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftNetworkFactory"
	)]
	async fn new_client(&mut self, target: NodeId, node: &RaftPeer) -> PeerClient {
		PeerClient {
			http: self.http.clone(),
			auth: self.auth.clone(),
			gossip: self.gossip.clone(),
			target,
			base: node.cluster_url.trim_end_matches('/').to_owned(),
		}
	}
}

/// Raft client for one peer.
pub struct PeerClient {
	http:   reqwest::Client,
	auth:   Arc<HeaderValue>,
	gossip: Gossip,
	target: NodeId,
	base:   String,
}

impl PeerClient {
	#[expect(
		clippy::result_large_err,
		reason = "openraft's RaftNetwork methods return this error type unboxed"
	)]
	async fn post<Req, Resp, E>(
		&self,
		path: &str,
		req: &Req,
		option: &RPCOption,
	) -> Result<Resp, RPCError<NodeId, RaftPeer, RaftError<NodeId, E>>>
	where
		Req: Serialize + Sync,
		Resp: DeserializeOwned,
		E: Error + DeserializeOwned,
	{
		let url = format!("{}/cluster/v1/raft/{path}", self.base);
		let response = self
			.http
			.post(url)
			.header(reqwest::header::AUTHORIZATION, (*self.auth).clone())
			.timeout(option.hard_ttl())
			.json(req)
			.send()
			.await
			.map_err(|e| {
				if e.is_connect() {
					RPCError::Unreachable(Unreachable::new(&e))
				} else {
					network(&e)
				}
			})?;
		if !response.status().is_success() {
			let status = response.status();
			let error =
				std::io::Error::other(format!("raft rpc {path} to {}: http {status}", self.target));
			return Err(network(&error));
		}
		let body: Result<Resp, RaftError<NodeId, E>> =
			response.json().await.map_err(|e| network(&e))?;
		body.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
	}
}

fn network<E: Error + 'static, F: Error>(error: &E) -> RPCError<NodeId, RaftPeer, F> {
	RPCError::Network(NetworkError::new(error))
}

impl RaftNetwork<TypeConfig> for PeerClient {
	async fn append_entries(
		&mut self,
		rpc: AppendEntriesRequest<TypeConfig>,
		option: RPCOption,
	) -> Result<AppendEntriesResponse<NodeId>, RPCError<NodeId, RaftPeer, RaftError<NodeId>>> {
		self.post("append", &rpc, &option).await
	}

	async fn install_snapshot(
		&mut self,
		rpc: InstallSnapshotRequest<TypeConfig>,
		option: RPCOption,
	) -> Result<
		InstallSnapshotResponse<NodeId>,
		RPCError<NodeId, RaftPeer, RaftError<NodeId, InstallSnapshotError>>,
	> {
		self.post("snapshot", &rpc, &option).await
	}

	async fn vote(
		&mut self,
		rpc: VoteRequest<NodeId>,
		option: RPCOption,
	) -> Result<VoteResponse<NodeId>, RPCError<NodeId, RaftPeer, RaftError<NodeId>>> {
		self.post("vote", &rpc, &option).await
	}

	/// Backoff after an unreachable error; long only while gossip reports the
	/// peer Dead/Left or unknown. The reconciler rebuilds replication streams
	/// when such a peer revives, so recovery does not wait out the delay.
	fn backoff(&self) -> Backoff {
		let gossip = self.gossip.clone();
		let target = self.target;
		let mut delay = Duration::ZERO;
		Backoff::new(std::iter::from_fn(move || {
			let live = gossip
				.member(target)
				.is_some_and(|m| matches!(m.state, MemberState::Alive | MemberState::Suspect));
			delay = next_backoff(delay, live);
			Some(delay)
		}))
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn backoff_grows_only_while_peer_is_down_and_caps_at_max() {
		let mut delay = Duration::ZERO;
		let mut seen = Vec::new();
		for _ in 0..8 {
			delay = next_backoff(delay, false);
			seen.push(delay.as_secs());
		}
		assert_eq!(seen, vec![1, 2, 4, 8, 16, 30, 30, 30]);
		assert_eq!(next_backoff(BACKOFF_MAX, true), BACKOFF_MIN, "a live peer is retried quickly");
		assert_eq!(next_backoff(BACKOFF_MIN, false), Duration::from_secs(1));
	}
}
