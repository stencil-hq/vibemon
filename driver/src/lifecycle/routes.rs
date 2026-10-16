//! KV records owned by the sandbox lifecycle (ARCHITECTURE.md §6): the live
//! route `sbx/<id>`, the admission index `team/<t>/sbx/<id>`, and the
//! per-sandbox TTL lock `lock/sbx/<id>`.

use std::time::Duration;

use axum::http::StatusCode;
use tokio::{task::JoinHandle, time::Instant};
use vmon_cluster::kv::{Compare, Kv, KvError, Op, Txn};
use vmon_node::api::SandboxRoute;

use crate::{
	error::{ApiError, ApiResult},
	state::AppState,
};

/// TTL of `lock/sbx/<id>` (§9.4); renewed while the holder works.
pub const LOCK_TTL: Duration = Duration::from_mins(1);
/// How often a held lock is renewed, counted from when the previous
/// successful write was proposed.
const LOCK_RENEW: Duration = Duration::from_secs(20);
/// Retry delay after a failed renewal.
const LOCK_RENEW_RETRY: Duration = Duration::from_secs(1);
/// Poll interval while waiting for a busy lock.
const LOCK_POLL: Duration = Duration::from_millis(100);
/// How long API operations wait for a concurrent transition to finish.
pub const LOCK_WAIT: Duration = Duration::from_mins(2);
/// Prefix of every route key.
pub const ROUTE_PREFIX: &str = "sbx/";

/// `sbx/<id>`.
pub fn route_key(sandbox_id: &str) -> String {
	format!("{ROUTE_PREFIX}{sandbox_id}")
}

/// `team/<teamID>/sbx/` (admission `CountBelow` prefix).
pub fn team_prefix(team_id: &str) -> String {
	format!("team/{team_id}/sbx/")
}

/// `team/<teamID>/sbx/<id>`.
pub fn index_key(team_id: &str, sandbox_id: &str) -> String {
	format!("{}{sandbox_id}", team_prefix(team_id))
}

/// `lock/sbx/<id>`.
pub fn lock_key(sandbox_id: &str) -> String {
	format!("lock/sbx/{sandbox_id}")
}

/// Reads and decodes a route (follower-local read).
pub async fn get_route(kv: &Kv, sandbox_id: &str) -> ApiResult<Option<SandboxRoute>> {
	decode(kv.get(&route_key(sandbox_id)).await?.map(|e| e.value))
}

/// A route with the revision it was read at (follower-local read), for
/// [`claim_route`].
pub async fn get_route_rev(kv: &Kv, sandbox_id: &str) -> ApiResult<Option<(SandboxRoute, u64)>> {
	let Some(entry) = kv.get(&route_key(sandbox_id)).await? else {
		return Ok(None);
	};
	Ok(decode(Some(entry.value))?.map(|route| (route, entry.mod_rev)))
}

/// Takes a sandbox out of the live catalog for a kill without taking its lock.
///
/// Deletes route and admission index iff no operation holds `lock/sbx/<id>`
/// and the route is unchanged since `rev`; returns `false` (nothing written)
/// otherwise. Afterwards no other transition can start (they all need the
/// route), so the rest of the kill runs unlocked.
pub async fn claim_route(kv: &Kv, sandbox_id: &str, team_id: &str, rev: u64) -> ApiResult<bool> {
	let key = route_key(sandbox_id);
	let txn = Txn {
		compare: vec![Compare::Missing(lock_key(sandbox_id)), Compare::ModRev {
			key: key.clone(),
			rev,
		}],
		success: vec![Op::Delete { key }, Op::Delete { key: index_key(team_id, sandbox_id) }],
		failure: Vec::new(),
	};
	Ok(kv.txn(txn).await?.succeeded)
}

/// Linearizable route read (leader read-index), for decisions that kill VMs.
pub async fn get_route_consistent(kv: &Kv, sandbox_id: &str) -> ApiResult<Option<SandboxRoute>> {
	decode(
		kv.get_consistent(&route_key(sandbox_id))
			.await?
			.map(|e| e.value),
	)
}

fn decode(value: Option<Vec<u8>>) -> ApiResult<Option<SandboxRoute>> {
	value
		.map(|bytes| {
			serde_json::from_slice(&bytes)
				.map_err(|e| ApiError::internal(format!("corrupt sandbox route: {e}")))
		})
		.transpose()
}

/// Every route in the cluster as `(sandbox_id, route)`; undecodable entries are
/// skipped.
pub async fn list_routes(kv: &Kv) -> ApiResult<Vec<(String, SandboxRoute)>> {
	Ok(kv
		.list(ROUTE_PREFIX)
		.await?
		.into_iter()
		.filter_map(|entry| {
			let id = entry.key.strip_prefix(ROUTE_PREFIX)?.to_owned();
			serde_json::from_slice(&entry.value)
				.ok()
				.map(|route| (id, route))
		})
		.collect())
}

/// Overwrites a route (caller holds the sandbox lock).
pub async fn put_route(kv: &Kv, sandbox_id: &str, route: &SandboxRoute) -> ApiResult<()> {
	kv.put(&route_key(sandbox_id), encode(route), None).await?;
	Ok(())
}

fn encode(route: &SandboxRoute) -> Vec<u8> {
	serde_json::to_vec(route).expect("sandbox route serializes")
}

/// Admission (§9.4): atomically checks that the sandbox has no route and the
/// team is below `max_concurrent`, then writes route + index. 429 when the
/// team is at its limit, 409 when a route already exists.
pub async fn admit(
	kv: &Kv,
	sandbox_id: &str,
	route: &SandboxRoute,
	max_concurrent: u32,
) -> ApiResult<()> {
	let key = route_key(sandbox_id);
	let txn = Txn {
		compare: vec![Compare::Missing(key.clone()), Compare::CountBelow {
			prefix: team_prefix(&route.team_id),
			limit:  u64::from(max_concurrent),
		}],
		success: vec![Op::Put { key: key.clone(), value: encode(route), ttl_ms: None }, Op::Put {
			key:    index_key(&route.team_id, sandbox_id),
			value:  Vec::new(),
			ttl_ms: None,
		}],
		failure: Vec::new(),
	};
	if kv.txn(txn).await?.succeeded {
		return Ok(());
	}
	if kv.get(&key).await?.is_some() {
		return Err(ApiError::conflict(format!("Sandbox {sandbox_id} is already running")));
	}
	Err(concurrency_limit(max_concurrent))
}

/// The upstream 429 for the concurrent-sandbox limit (not retried by SDKs).
pub fn concurrency_limit(max_concurrent: u32) -> ApiError {
	ApiError::new(
		StatusCode::TOO_MANY_REQUESTS,
		format!(
			"you have reached the maximum number of concurrent E2B sandboxes ({max_concurrent}). If you need more, \
			 please visit 'https://e2b.dev/docs/billing'"
		),
	)
	.with_code("sandbox_concurrency_limit")
}

/// Deletes route and admission index together.
pub async fn remove_route(kv: &Kv, sandbox_id: &str, team_id: &str) -> ApiResult<()> {
	let txn = Txn {
		compare: Vec::new(),
		success: vec![Op::Delete { key: route_key(sandbox_id) }, Op::Delete {
			key: index_key(team_id, sandbox_id),
		}],
		failure: Vec::new(),
	};
	kv.txn(txn).await?;
	Ok(())
}

/// Outcome of [`hold_lease`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Lease {
	/// The holder still had the lease; its TTL restarted.
	Extended,
	/// The lease was absent (never taken, released, or expired) and is now
	/// the holder's.
	Taken,
	/// Another holder has the lease.
	Lost,
}

/// Holds the TTL lease `key` for `holder`: extends it while `holder` owns it
/// and takes it when nobody does.
///
/// A TTL counts from the leader's clock when the write is proposed, so under
/// raft commit latency a renewal can apply after the lease has already
/// expired. That is a lapse, not a takeover: the lease is re-taken unless
/// another holder got it first.
pub async fn hold_lease(
	kv: &Kv,
	key: &str,
	holder: &[u8],
	ttl: Duration,
) -> Result<Lease, KvError> {
	let put = Op::Put { key: key.to_owned(), value: holder.to_vec(), ttl_ms: Some(ttl_ms(ttl)) };
	let extend = Txn {
		compare: vec![Compare::Value { key: key.to_owned(), value: holder.to_vec() }],
		success: vec![put.clone()],
		failure: Vec::new(),
	};
	if kv.txn(extend).await?.succeeded {
		return Ok(Lease::Extended);
	}
	let take = Txn {
		compare: vec![Compare::Missing(key.to_owned())],
		success: vec![put],
		failure: Vec::new(),
	};
	Ok(if kv.txn(take).await?.succeeded {
		Lease::Taken
	} else {
		Lease::Lost
	})
}

/// Whether some operation currently holds `lock/sbx/<id>`.
pub async fn is_locked(kv: &Kv, sandbox_id: &str) -> ApiResult<bool> {
	Ok(kv.get(&lock_key(sandbox_id)).await?.is_some())
}

/// A held `lock/sbx/<id>` lease, renewed in the background until released.
pub struct SandboxLock {
	kv:     Kv,
	key:    String,
	holder: Vec<u8>,
	renew:  Option<JoinHandle<()>>,
}

impl SandboxLock {
	/// Takes the lock, polling up to `wait` while another operation holds it.
	/// `Duration::ZERO` tries once. 409 when still busy afterwards.
	pub async fn acquire(state: &AppState, sandbox_id: &str, wait: Duration) -> ApiResult<Self> {
		let kv = state.cluster.kv.clone();
		let key = lock_key(sandbox_id);
		let holder =
			format!("{}/{}", state.cluster.local_id(), uuid::Uuid::new_v4().simple()).into_bytes();
		let deadline = Instant::now() + wait;
		loop {
			// The TTL runs from the proposal, not from when the reply arrives.
			let proposed = Instant::now();
			let txn = Txn {
				compare: vec![Compare::Missing(key.clone())],
				success: vec![Op::Put {
					key:    key.clone(),
					value:  holder.clone(),
					ttl_ms: Some(ttl_ms(LOCK_TTL)),
				}],
				failure: Vec::new(),
			};
			if kv.txn(txn).await?.succeeded {
				let renew = tokio::spawn(renew_loop(kv.clone(), key.clone(), holder.clone(), proposed));
				return Ok(Self { kv, key, holder, renew: Some(renew) });
			}
			if Instant::now() >= deadline {
				return Err(ApiError::conflict(format!(
					"Sandbox \"{sandbox_id}\" is changing state, please retry"
				)));
			}
			tokio::time::sleep(LOCK_POLL).await;
		}
	}

	/// Releases the lock if this holder still owns it.
	pub async fn release(mut self) {
		if let Some(renew) = self.renew.take() {
			renew.abort();
		}
		release(&self.kv, &self.key, &self.holder).await;
	}
}

impl Drop for SandboxLock {
	fn drop(&mut self) {
		let Some(renew) = self.renew.take() else {
			return;
		};
		renew.abort();
		// Dropped without `release` (error path or cancelled request): free it in
		// the background.
		let (kv, key, holder) = (self.kv.clone(), self.key.clone(), std::mem::take(&mut self.holder));
		if let Ok(handle) = tokio::runtime::Handle::try_current() {
			handle.spawn(async move { release(&kv, &key, &holder).await });
		}
	}
}

async fn release(kv: &Kv, key: &str, holder: &[u8]) {
	let txn = Txn {
		compare: vec![Compare::Value { key: key.to_owned(), value: holder.to_vec() }],
		success: vec![Op::Delete { key: key.to_owned() }],
		failure: Vec::new(),
	};
	if let Err(error) = kv.txn(txn).await {
		tracing::warn!(%error, key, "releasing sandbox lock failed; it expires with its TTL");
	}
}

/// Renews a held lock every [`LOCK_RENEW`] after the last successful write
/// was proposed (immediately when that write took longer to commit), so slow
/// KV commits do not let it lapse; a lapsed lock nobody else took is re-taken.
async fn renew_loop(kv: Kv, key: String, holder: Vec<u8>, proposed: Instant) {
	let mut next = proposed + LOCK_RENEW;
	loop {
		tokio::time::sleep_until(next).await;
		let proposed = Instant::now();
		match hold_lease(&kv, &key, &holder, LOCK_TTL).await {
			Ok(Lease::Extended) => next = proposed + LOCK_RENEW,
			Ok(Lease::Taken) => {
				tracing::warn!(key, "sandbox lock lapsed before its renewal applied; re-took it");
				next = proposed + LOCK_RENEW;
			},
			Ok(Lease::Lost) => {
				tracing::warn!(key, "sandbox lock lost before the operation finished");
				return;
			},
			Err(error) => {
				tracing::warn!(%error, key, "renewing sandbox lock failed");
				next = Instant::now() + LOCK_RENEW_RETRY;
			},
		}
	}
}

fn ttl_ms(ttl: Duration) -> u64 {
	u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX)
}
