//! Cluster HTTP for raft and KV: axum handlers (`/cluster/v1/{raft,kv}/*`) and
//! the reqwest client used to forward KV calls to drivers / the leader.
//!
//! KV replies are always HTTP 200 with `{"ok": T}` or `{"err": KvError}`; any
//! other status is a transport failure.

use std::sync::Arc;

use axum::{
	Json, Router,
	extract::{DefaultBodyLimit, State},
	http::HeaderMap,
	routing::post,
};
use openraft::raft::{AppendEntriesRequest, InstallSnapshotRequest, VoteRequest};
use reqwest::header::HeaderValue;
use serde::{Deserialize, Serialize, de::DeserializeOwned};

use super::{
	Entry, KvError, Range, Txn, TxnResult,
	raft::{RaftKv, TypeConfig},
};
use crate::meta::NodeId;

/// Marks a call already forwarded once by a driver; the receiver must not
/// forward again.
const FORWARDED_HEADER: &str = "x-vmon-kv-forwarded";

/// Raft batches and snapshot chunks are far larger than axum's 2 MiB default.
const RAFT_BODY_LIMIT: usize = 256 << 20;

/// Body of `POST /cluster/v1/kv/get`.
#[derive(Debug, Serialize, Deserialize)]
pub struct GetRequest {
	pub key:        String,
	pub consistent: bool,
}

/// Body of `POST /cluster/v1/kv/list`.
#[derive(Debug, Serialize, Deserialize)]
pub struct ListRequest {
	pub prefix: String,
}

#[derive(Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Reply<T> {
	Ok(T),
	Err(KvError),
}

impl<T> From<Result<T, KvError>> for Reply<T> {
	fn from(result: Result<T, KvError>) -> Self {
		match result {
			Ok(value) => Self::Ok(value),
			Err(error) => Self::Err(error),
		}
	}
}

/// Whether a KV call is the first hop (receiver may forward) or a
/// driver-to-leader forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hop {
	Entry,
	Forwarded,
}

impl Hop {
	fn from_headers(headers: &HeaderMap) -> Self {
		if headers.contains_key(FORWARDED_HEADER) {
			Self::Forwarded
		} else {
			Self::Entry
		}
	}
}

/// `/cluster/v1/raft/*` and `/cluster/v1/kv/*` served by a driver (auth applied
/// by the caller).
pub fn routes(kv: Arc<RaftKv>) -> Router {
	Router::new()
		.route("/cluster/v1/raft/append", post(append))
		.route("/cluster/v1/raft/vote", post(vote))
		.route("/cluster/v1/raft/snapshot", post(snapshot))
		.route("/cluster/v1/kv/get", post(kv_get))
		.route("/cluster/v1/kv/list", post(kv_list))
		.route("/cluster/v1/kv/range", post(kv_range))
		.route("/cluster/v1/kv/sync", post(kv_sync))
		.route("/cluster/v1/kv/txn", post(kv_txn))
		.layer(DefaultBodyLimit::max(RAFT_BODY_LIMIT))
		.with_state(kv)
}

async fn append(
	State(kv): State<Arc<RaftKv>>,
	Json(req): Json<AppendEntriesRequest<TypeConfig>>,
) -> Json<impl Serialize> {
	Json(kv.raft().append_entries(req).await)
}

async fn vote(
	State(kv): State<Arc<RaftKv>>,
	Json(req): Json<VoteRequest<NodeId>>,
) -> Json<impl Serialize> {
	Json(kv.raft().vote(req).await)
}

async fn snapshot(
	State(kv): State<Arc<RaftKv>>,
	Json(req): Json<InstallSnapshotRequest<TypeConfig>>,
) -> Json<impl Serialize> {
	Json(kv.raft().install_snapshot(req).await)
}

async fn kv_get(
	State(kv): State<Arc<RaftKv>>,
	headers: HeaderMap,
	Json(req): Json<GetRequest>,
) -> Json<Reply<Option<Entry>>> {
	let hop = Hop::from_headers(&headers);
	let result = if req.consistent {
		kv.get_consistent(&req.key, hop == Hop::Forwarded).await
	} else {
		kv.get_served(&req.key).await
	};
	Json(result.into())
}

async fn kv_list(
	State(kv): State<Arc<RaftKv>>,
	Json(req): Json<ListRequest>,
) -> Json<Reply<Vec<Entry>>> {
	Json(kv.list_served(&req.prefix).await.into())
}

async fn kv_range(
	State(kv): State<Arc<RaftKv>>,
	Json(range): Json<Range>,
) -> Json<Reply<Vec<Entry>>> {
	Json(kv.range_served(&range).await.into())
}

/// Leader's applied revision (read barrier of a follower).
async fn kv_sync(State(kv): State<Arc<RaftKv>>, headers: HeaderMap) -> Json<Reply<u64>> {
	let hop = Hop::from_headers(&headers);
	Json(kv.leader_revision(hop == Hop::Forwarded).await.into())
}

async fn kv_txn(
	State(kv): State<Arc<RaftKv>>,
	headers: HeaderMap,
	Json(txn): Json<Txn>,
) -> Json<Reply<TxnResult>> {
	let hop = Hop::from_headers(&headers);
	Json(kv.txn(txn, hop == Hop::Forwarded).await.into())
}

/// Failure of a forwarded KV call.
#[derive(Debug)]
pub enum CallError {
	/// The request never reached the peer (safe to retry elsewhere).
	Connect(String),
	/// The peer answered with a KV error, or the outcome is unknown.
	Kv(KvError),
}

/// HTTP client for `/cluster/v1/kv/*`.
#[derive(Clone)]
pub struct KvClient {
	http: reqwest::Client,
	auth: Arc<HeaderValue>,
}

impl KvClient {
	pub const fn new(http: reqwest::Client, auth: Arc<HeaderValue>) -> Self {
		Self { http, auth }
	}

	/// Posts `req` to `{base}/cluster/v1/kv/{op}`.
	pub async fn call<Req, Resp>(
		&self,
		base: &str,
		op: &str,
		req: &Req,
		hop: Hop,
	) -> Result<Resp, CallError>
	where
		Req: Serialize + Sync + ?Sized,
		Resp: DeserializeOwned,
	{
		let url = format!("{}/cluster/v1/kv/{op}", base.trim_end_matches('/'));
		let mut request = self
			.http
			.post(url)
			.header(reqwest::header::AUTHORIZATION, (*self.auth).clone())
			.json(req);
		if hop == Hop::Forwarded {
			request = request.header(FORWARDED_HEADER, "1");
		}
		let response = request.send().await.map_err(|e| {
			if e.is_connect() {
				CallError::Connect(e.to_string())
			} else {
				CallError::Kv(KvError::Unavailable(format!("kv call to {base}: {e}")))
			}
		})?;
		let status = response.status();
		if status == reqwest::StatusCode::UNAUTHORIZED {
			return Err(CallError::Kv(KvError::Rejected(format!("kv call to {base}: unauthorized"))));
		}
		if !status.is_success() {
			return Err(CallError::Kv(KvError::Unavailable(format!(
				"kv call to {base}: http {status}"
			))));
		}
		match response.json::<Reply<Resp>>().await {
			Ok(Reply::Ok(value)) => Ok(value),
			Ok(Reply::Err(error)) => Err(CallError::Kv(error)),
			Err(error) => Err(CallError::Kv(KvError::Io(format!("kv reply from {base}: {error}")))),
		}
	}
}
