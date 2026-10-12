//! Keeps a proxied response open while its sandbox is paused.
//!
//! Pausing stops the VM, which ends every tunnel to it. If that ending reached
//! the client as a failed stream, the SDKs would probe envd `/health` through
//! the ingress (`handleRpcErrorWithHealthCheck`), and for an `autoResume`
//! sandbox that probe is ingress traffic that resumes it. A sandbox with an
//! open background command would then wake up right after every pause.
//! Instead, a response that breaks while its sandbox is pausing or paused
//! stays open, without data, the way a frozen guest's connection does. The
//! failure is released once the sandbox is running again (the old guest
//! connection is gone), is killed, or after [`HOLD_LIMIT`].

use std::{
	future::Future,
	pin::Pin,
	task::{Context, Poll, ready},
	time::Duration,
};

use axum::body::{Body, Bytes};
use hyper::body::{Frame, SizeHint};
use vmon_node::api::{RouteState, SandboxRoute};

use crate::{lifecycle::routes::route_key, state::AppState, store::sandboxes::SandboxState};

/// Longest hold; matches the upstream proxy's idle timeout (620 s).
const HOLD_LIMIT: Duration = Duration::from_secs(620);
/// How often the sandbox state is re-read while holding.
const HOLD_POLL: Duration = Duration::from_millis(250);

/// What a broken response waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Hold {
	/// Pausing or paused: keep the response open.
	Wait,
	/// Running, killed, or unknown: report the failure now.
	Release,
}

/// Decides from the sandbox's live route and, without one, its durable state.
pub fn decide(route: Option<&SandboxRoute>, durable: Option<SandboxState>) -> Hold {
	match route.map(|route| route.state) {
		Some(RouteState::Pausing | RouteState::Resuming) => Hold::Wait,
		Some(RouteState::Running | RouteState::Snapshotting | RouteState::Killing) => Hold::Release,
		None if durable == Some(SandboxState::Paused) => Hold::Wait,
		None => Hold::Release,
	}
}

/// Wait that runs before a held failure is reported.
type HoldWait = Pin<Box<dyn Future<Output = ()> + Send>>;

/// Response body that defers an upstream failure while the sandbox is paused.
pub struct HoldWhilePaused {
	inner:   Body,
	state:   AppState,
	sandbox: String,
	/// The upstream failure and the wait before reporting it.
	held:    Option<(axum::Error, HoldWait)>,
}

impl HoldWhilePaused {
	/// Wraps a proxied response body of `sandbox`.
	pub fn new(inner: Body, state: AppState, sandbox: String) -> Self {
		Self { inner, state, sandbox, held: None }
	}
}

impl hyper::body::Body for HoldWhilePaused {
	type Data = Bytes;
	type Error = axum::Error;

	fn poll_frame(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
	) -> Poll<Option<Result<Frame<Bytes>, axum::Error>>> {
		if self.held.is_none() {
			match ready!(Pin::new(&mut self.inner).poll_frame(cx)) {
				Some(Err(error)) => {
					let wait = Box::pin(wait_while_paused(self.state.clone(), self.sandbox.clone()));
					self.held = Some((error, wait));
				},
				other => return Poll::Ready(other),
			}
		}
		let (_, wait) = self.held.as_mut().expect("held is set above");
		ready!(wait.as_mut().poll(cx));
		let (error, _) = self.held.take().expect("held is set above");
		Poll::Ready(Some(Err(error)))
	}

	fn is_end_stream(&self) -> bool {
		self.held.is_none() && self.inner.is_end_stream()
	}

	fn size_hint(&self) -> SizeHint {
		self.inner.size_hint()
	}
}

/// Resolves once the sandbox is no longer pausing or paused (or after
/// [`HOLD_LIMIT`]); store or KV failures release the hold.
async fn wait_while_paused(state: AppState, sandbox: String) {
	let wait = async {
		let key = route_key(&sandbox);
		let mut held = false;
		loop {
			let Ok(entry) = state.cluster.kv.get(&key).await else {
				return;
			};
			let route =
				entry.and_then(|entry| serde_json::from_slice::<SandboxRoute>(&entry.value).ok());
			let durable = match route {
				Some(_) => None,
				None => match state.store.sandbox(&sandbox).await {
					Ok(row) => row.map(|row| row.state()),
					Err(_) => return,
				},
			};
			if decide(route.as_ref(), durable) == Hold::Release {
				return;
			}
			if !held {
				held = true;
				tracing::debug!(%sandbox, "upstream ended while the sandbox pauses; holding the response");
			}
			tokio::time::sleep(HOLD_POLL).await;
		}
	};
	let _ = tokio::time::timeout(HOLD_LIMIT, wait).await;
}

#[cfg(test)]
mod tests {
	use vmon_cluster::NodeId;

	use super::*;

	fn route(state: RouteState) -> SandboxRoute {
		SandboxRoute {
			team_id: "team".into(),
			node_id: NodeId(1),
			template_id: "tpl".into(),
			build_id: "build".into(),
			state,
			started_at_ms: 0,
			end_at_ms: u64::MAX,
			auto_pause: true,
			auto_resume: true,
			allow_public_traffic: true,
			traffic_token: None,
			mask_request_host: None,
			https_ports: Vec::new(),
			vcpus: 1,
			memory_mb: 512,
			envd_version: "0.6.4".into(),
		}
	}

	#[test]
	fn holds_only_while_pausing_or_paused() {
		assert_eq!(decide(Some(&route(RouteState::Pausing)), None), Hold::Wait);
		assert_eq!(decide(Some(&route(RouteState::Resuming)), None), Hold::Wait);
		assert_eq!(decide(None, Some(SandboxState::Paused)), Hold::Wait);
		assert_eq!(decide(Some(&route(RouteState::Running)), None), Hold::Release, "guest closed it");
		assert_eq!(decide(Some(&route(RouteState::Killing)), None), Hold::Release);
		assert_eq!(decide(Some(&route(RouteState::Snapshotting)), None), Hold::Release);
		assert_eq!(decide(None, Some(SandboxState::Killed)), Hold::Release);
		assert_eq!(decide(None, None), Hold::Release, "unknown sandbox");
	}
}
