//! Raft RPC client: openraft network over cluster HTTP (`/cluster/v1/raft/*`).

use std::{collections::HashMap, error::Error, future::Future, sync::Arc, time::Duration};

use bytes::Bytes;
use openraft::{
	RaftNetworkFactory, RaftNetworkV2,
	error::{
		Fatal, NetworkError, RPCError, RaftError, ReplicationClosed, StreamingError, Unreachable,
	},
	network::{Backoff, RPCOption},
	raft::{
		AppendEntriesRequest, AppendEntriesResponse, SnapshotResponse, VoteRequest, VoteResponse,
	},
};
use parking_lot::RwLock;
use reqwest::header::HeaderValue;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::raft::{RaftPeer, Snapshot, SnapshotMeta, TypeConfig, Vote};
use crate::{
	gossip::{Gossip, MemberState},
	meta::{MemberMeta, NodeId},
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

/// First line of a `/cluster/v1/raft/snapshot` body (JSON); the serialized
/// snapshot data follows the newline.
#[derive(Serialize, Deserialize)]
pub struct SnapshotHeader {
	/// The sending leader's vote.
	pub vote: Vote,
	pub meta: SnapshotMeta,
}

impl SnapshotHeader {
	/// Splits a snapshot request body into its header and snapshot data.
	pub fn decode(mut body: Bytes) -> Option<(Self, Bytes)> {
		let split = body.iter().position(|byte| *byte == b'\n')?;
		let data = body.split_off(split + 1);
		let header = serde_json::from_slice(&body[..split]).ok()?;
		Some((header, data))
	}
}

/// Raft URL recorded in raft membership for every peer this process sends
/// RPCs to; used when gossip cannot resolve the peer (see
/// `PeerClient::base_url`).
///
/// openraft keeps a peer's replication stream, and so its client, across
/// membership changes; a peer re-addressed by the reconciler is reached
/// through this map instead of a new client.
#[derive(Clone, Default)]
pub struct PeerUrls(Arc<RwLock<HashMap<NodeId, String>>>);

impl PeerUrls {
	/// Records `url` as the base URL of `id`.
	pub fn set(&self, id: NodeId, url: &str) {
		let url = url.trim_end_matches('/');
		let mut urls = self.0.write();
		if urls.get(&id).is_none_or(|known| known != url) {
			urls.insert(id, url.to_owned());
		}
	}

	/// Recorded base URL of `id`.
	fn base(&self, id: NodeId) -> Option<String> {
		self.0.read().get(&id).cloned()
	}
}

/// Creates per-peer raft clients sharing one HTTP pool.
pub struct Network {
	http:   reqwest::Client,
	auth:   Arc<HeaderValue>,
	gossip: Gossip,
	urls:   PeerUrls,
}

impl Network {
	pub const fn new(
		http: reqwest::Client,
		auth: Arc<HeaderValue>,
		gossip: Gossip,
		urls: PeerUrls,
	) -> Self {
		Self { http, auth, gossip, urls }
	}
}

impl RaftNetworkFactory<TypeConfig> for Network {
	type Network = PeerClient;

	#[expect(
		clippy::unused_async_trait_impl,
		reason = "signature fixed by openraft's RaftNetworkFactory"
	)]
	async fn new_client(&mut self, target: NodeId, node: &RaftPeer) -> PeerClient {
		self.urls.set(target, &node.cluster_url);
		PeerClient {
			http: self.http.clone(),
			auth: self.auth.clone(),
			gossip: self.gossip.clone(),
			urls: self.urls.clone(),
			target,
			meta: None,
		}
	}
}

/// Raft client for one peer.
pub struct PeerClient {
	http:   reqwest::Client,
	auth:   Arc<HeaderValue>,
	gossip: Gossip,
	urls:   PeerUrls,
	target: NodeId,
	/// `(meta_version, meta)` of the target's last decoded gossip metadata.
	meta:   Option<(u64, MemberMeta)>,
}

impl PeerClient {
	/// Base URL of the target: its cluster listener at this process's gossip
	/// route to it (LAN first, failed over by probes), so every driver dials
	/// peers its own way; else the URL raft membership recorded.
	fn base_url(&mut self) -> Option<String> {
		self.gossip_url().or_else(|| self.urls.base(self.target))
	}

	fn gossip_url(&mut self) -> Option<String> {
		let member = self.gossip.member(self.target)?;
		if self
			.meta
			.as_ref()
			.is_none_or(|(version, _)| *version != member.meta_version)
		{
			self.meta = Some((member.meta_version, MemberMeta::decode(&member.meta)?));
		}
		self
			.meta
			.as_ref()
			.map(|(_, meta)| meta.cluster_url(&member))
	}

	/// Posts `body` of `content_type` to `/cluster/v1/raft/{path}` and decodes
	/// the peer's `Result<Resp, E>`; a remote error counts as unreachable (the
	/// peer's raft is stopping).
	async fn post<Resp, E>(
		&mut self,
		path: &str,
		body: reqwest::Body,
		content_type: &'static str,
		option: &RPCOption,
	) -> Result<Resp, RPCError<TypeConfig>>
	where
		Resp: DeserializeOwned,
		E: Error + DeserializeOwned,
	{
		let Some(base) = self.base_url() else {
			return Err(RPCError::Unreachable(Unreachable::from_string(format!(
				"raft rpc {path} to {}: no known address",
				self.target
			))));
		};
		let response = self
			.http
			.post(format!("{base}/cluster/v1/raft/{path}"))
			.header(reqwest::header::AUTHORIZATION, (*self.auth).clone())
			.header(reqwest::header::CONTENT_TYPE, content_type)
			.timeout(option.hard_ttl())
			.body(body)
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
		let body: Result<Resp, E> = response.json().await.map_err(|e| network(&e))?;
		body.map_err(|e| {
			RPCError::Unreachable(Unreachable::from_string(format!(
				"raft rpc {path} to {}: remote error: {e}",
				self.target
			)))
		})
	}

	async fn post_json<Req, Resp>(
		&mut self,
		path: &str,
		req: &Req,
		option: &RPCOption,
	) -> Result<Resp, RPCError<TypeConfig>>
	where
		Req: Serialize + Sync,
		Resp: DeserializeOwned,
	{
		let body = serde_json::to_vec(req).map_err(|e| network(&e))?;
		self
			.post::<Resp, RaftError<TypeConfig>>(path, body.into(), "application/json", option)
			.await
	}
}

fn network<E: Error + 'static>(error: &E) -> RPCError<TypeConfig> {
	RPCError::Network(NetworkError::new(error))
}

impl RaftNetworkV2<TypeConfig> for PeerClient {
	type SnapshotData = Vec<u8>;

	async fn append_entries(
		&mut self,
		rpc: AppendEntriesRequest<TypeConfig>,
		option: RPCOption,
	) -> Result<AppendEntriesResponse<TypeConfig>, RPCError<TypeConfig>> {
		self.post_json("append", &rpc, &option).await
	}

	async fn vote(
		&mut self,
		rpc: VoteRequest<TypeConfig>,
		option: RPCOption,
	) -> Result<VoteResponse<TypeConfig>, RPCError<TypeConfig>> {
		self.post_json("vote", &rpc, &option).await
	}

	/// Sends the whole snapshot in one request: a [`SnapshotHeader`] line,
	/// then the data, streamed without copying it.
	async fn full_snapshot(
		&mut self,
		vote: Vote,
		snapshot: Snapshot,
		cancel: impl Future<Output = ReplicationClosed> + Send + 'static,
		option: RPCOption,
	) -> Result<SnapshotResponse<TypeConfig>, StreamingError<TypeConfig>> {
		let header = SnapshotHeader { vote, meta: snapshot.meta };
		let mut head = serde_json::to_vec(&header).map_err(|e| network(&e))?;
		head.push(b'\n');
		let chunks: [Result<Bytes, std::io::Error>; 2] =
			[Ok(Bytes::from(head)), Ok(Bytes::from(snapshot.snapshot))];
		let body = reqwest::Body::wrap_stream(futures::stream::iter(chunks));
		tokio::select! {
			result = self.post::<_, Fatal<TypeConfig>>(
				"snapshot",
				body,
				"application/octet-stream",
				&option,
			) => result.map_err(StreamingError::from),
			closed = cancel => Err(StreamingError::Closed(closed)),
		}
	}

	/// Backoff after an unreachable error; long only while gossip reports the
	/// peer Dead/Left or unknown. The reconciler resets the backoff of such a
	/// peer when it revives, so recovery does not wait out the delay.
	fn backoff(&self) -> Option<Backoff> {
		let gossip = self.gossip.clone();
		let target = self.target;
		let mut delay = Duration::ZERO;
		Some(Backoff::new(std::iter::from_fn(move || {
			let live = gossip
				.member(target)
				.is_some_and(|m| matches!(m.state, MemberState::Alive | MemberState::Suspect));
			delay = next_backoff(delay, live);
			Some(delay)
		})))
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
