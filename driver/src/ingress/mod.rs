//! Public front door of the driver (ARCHITECTURE.md §3, §9.5): routes each
//! request either to a sandbox port (ingress proxy) or to the E2B API, and
//! applies the browser CORS policy to both.

pub mod hold;
pub mod listener;
pub mod policy;
pub mod proxy;
pub mod response;
pub mod target;

use std::time::{Duration, Instant};

use axum::{Router, extract::Request, http::StatusCode, response::Response};
use tower::ServiceExt;
use vmon_node::api::{RouteState, SandboxRoute};

use self::{
	response::ProxyError,
	target::{Dispatch, Target},
};
use crate::state::AppState;

/// How long ingress waits for a sandbox that another request is resuming.
const RESUMING_WAIT: Duration = Duration::from_mins(1);
/// Poll interval while waiting for a `resuming` route to become `running`.
const RESUMING_POLL: Duration = Duration::from_millis(200);

/// Puts sandbox ingress and CORS in front of the API router; called by
/// [`crate::Driver::public_router`] for the public listeners.
pub fn wrap(state: AppState, api: Router) -> Router {
	Router::new().fallback_service(tower::service_fn(move |req: Request| {
		let state = state.clone();
		let api = api.clone();
		async move { Ok::<_, std::convert::Infallible>(handle(state, api, req).await) }
	}))
}

async fn handle(state: AppState, api: Router, req: Request) -> Response {
	match target::dispatch(req.uri(), req.headers()) {
		Dispatch::Api => {
			if response::is_preflight(req.method(), req.headers()) {
				return response::preflight(req.headers());
			}
			let Ok(mut response) = api.oneshot(req).await;
			response::allow_any_origin(response.headers_mut());
			response
		},
		Dispatch::Invalid(message) => response::bad_request(message),
		Dispatch::Sandbox(target) => ingress(&state, target, req).await,
	}
}

/// Route-resolution failure.
enum Resolve {
	Proxy(ProxyError),
	Internal(String),
}

/// Proxies one request to a sandbox port after the access policies.
async fn ingress(state: &AppState, target: Target, req: Request) -> Response {
	if policy::is_blocked_control_path(target.port, req.uri().path()) {
		return ProxyError::reserved_path(req.uri().path()).render(req.headers());
	}
	let preflight = response::is_preflight(req.method(), req.headers());
	let headers = req.headers().clone();
	let fail = |error: Resolve| match error {
		_ if preflight => response::preflight(&headers),
		Resolve::Proxy(error) => error.render(&headers),
		Resolve::Internal(message) => response::routing_failed(&message),
	};
	let route = match resolve(state, &target, req.uri().path()).await {
		Ok(route) => route,
		Err(error) => return fail(error),
	};
	if let Err(denied) = policy::check_traffic_token(&route, target.port, req.headers()) {
		return ProxyError::traffic_denied(&target.sandbox_id, denied.message()).render(&headers);
	}
	let node = match state.node(route.node_id) {
		Ok(node) => node,
		Err(error) => {
			tracing::debug!(sandbox = %target.sandbox_id, node = %route.node_id, message = %error.message, "owning node unavailable");
			return fail(Resolve::Proxy(ProxyError::not_found(&target.sandbox_id)));
		},
	};
	let upstream = proxy::Upstream {
		node:        &node,
		target:      &target,
		https:       target.port != policy::ENVD_PORT && route.https_ports.contains(&target.port),
		masked_host: policy::masked_host(&route, target.port),
	};
	let response = proxy::forward(upstream, req).await;
	if response.status() == StatusCode::SWITCHING_PROTOCOLS {
		return response;
	}
	let mut response = response.map(|body| {
		axum::body::Body::new(hold::HoldWhilePaused::new(
			body,
			state.clone(),
			target.sandbox_id.clone(),
		))
	});
	response::allow_any_origin(response.headers_mut());
	response
}

/// Finds the live route of a sandbox: local KV read, waiting out `resuming`,
/// and auto-resuming paused sandboxes that opted in.
async fn resolve(state: &AppState, target: &Target, path: &str) -> Result<SandboxRoute, Resolve> {
	let sandbox_id = target.sandbox_id.as_str();
	let key = format!("sbx/{sandbox_id}");
	let deadline = Instant::now() + RESUMING_WAIT;
	loop {
		let entry = state
			.cluster
			.kv
			.get(&key)
			.await
			.map_err(|e| Resolve::Internal(e.to_string()))?;
		let Some(entry) = entry else {
			return resume(state, target, path).await;
		};
		let route: SandboxRoute = serde_json::from_slice(&entry.value)
			.map_err(|e| Resolve::Internal(format!("corrupt route {key}: {e}")))?;
		match route.state {
			RouteState::Running | RouteState::Snapshotting => return Ok(route),
			RouteState::Pausing if route.auto_resume => return resume(state, target, path).await,
			RouteState::Killing | RouteState::Pausing => {
				return Err(Resolve::Proxy(ProxyError::not_found(sandbox_id)));
			},
			RouteState::Resuming if Instant::now() >= deadline => {
				return Err(Resolve::Proxy(ProxyError::transitioning(sandbox_id)));
			},
			RouteState::Resuming => tokio::time::sleep(RESUMING_POLL).await,
		}
	}
}

/// Auto-resume through the lifecycle state machine, mapping its API errors
/// onto the upstream proxy bodies.
async fn resume(state: &AppState, target: &Target, path: &str) -> Result<SandboxRoute, Resolve> {
	let sandbox_id = target.sandbox_id.as_str();
	tracing::info!(
		sandbox = sandbox_id,
		port = target.port,
		path,
		"ingress traffic for a sandbox without a running route; trying auto-resume"
	);
	// Boxed: the lifecycle future is deep enough to overflow the `Send`
	// evaluation of the router's service future otherwise.
	let resumed: std::pin::Pin<
		Box<dyn Future<Output = crate::error::ApiResult<SandboxRoute>> + Send + '_>,
	> = Box::pin(crate::lifecycle::resume_for_ingress(state, sandbox_id));
	resumed.await.map_err(|error| {
		let proxy = match error.status {
			StatusCode::NOT_FOUND | StatusCode::GONE => ProxyError::not_found(sandbox_id),
			StatusCode::CONFLICT => ProxyError::transitioning(sandbox_id),
			StatusCode::TOO_MANY_REQUESTS => ProxyError::limit(sandbox_id, &error.message),
			StatusCode::FORBIDDEN | StatusCode::UNAUTHORIZED => ProxyError::resume_denied(sandbox_id),
			_ => return Resolve::Internal(error.message),
		};
		Resolve::Proxy(proxy)
	})
}
