//! Ingress access policies applied before proxying (ARCHITECTURE.md §9.5):
//! envd control-path blocking, private-traffic tokens, and host masking.

use axum::http::HeaderMap;
use percent_encoding::percent_decode_str;
use vmon_node::api::SandboxRoute;

use crate::crypto::ct_eq;

/// Guest port of envd.
pub const ENVD_PORT: u16 = 49983;

/// Header carrying the private-traffic token.
pub const TRAFFIC_TOKEN_HEADER: &str = "e2b-traffic-access-token";

/// envd endpoints reserved for the control plane (never public).
const CONTROL_PATHS: [&str; 7] =
	["/init", "/freeze", "/unfreeze", "/collapse", "/fsfreeze", "/fsthaw", "/upgrade"];

/// Whether a request path on `port` hits a reserved envd control endpoint;
/// method-agnostic, compared on the decoded, cleaned path.
pub fn is_blocked_control_path(port: u16, path: &str) -> bool {
	port == ENVD_PORT && CONTROL_PATHS.contains(&clean_path(path).as_str())
}

/// Lexically cleaned absolute path (Go `path.Clean` semantics) of the
/// percent-decoded request path.
fn clean_path(path: &str) -> String {
	let decoded = percent_decode_str(path).decode_utf8_lossy();
	let mut parts: Vec<&str> = Vec::new();
	for segment in decoded.split('/') {
		match segment {
			"" | "." => {},
			".." => {
				parts.pop();
			},
			other => parts.push(other),
		}
	}
	format!("/{}", parts.join("/"))
}

/// Why a private sandbox port refused a request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrafficDenied {
	Missing,
	Invalid,
}

impl TrafficDenied {
	/// Upstream wording of the 403 message.
	pub const fn message(self) -> &'static str {
		match self {
			Self::Missing => {
				"Sandbox is secured with traffic access token. Token header 'e2b-traffic-access-token' \
				 is missing"
			},
			Self::Invalid => {
				"Sandbox is secured with traffic access token. Provided token in header \
				 'e2b-traffic-access-token' is invalid"
			},
		}
	}
}

/// Enforces `allowPublicTraffic=false`: non-envd ports require the route's
/// traffic token (constant-time compare). A private route without a stored
/// token refuses every request.
pub fn check_traffic_token(
	route: &SandboxRoute,
	port: u16,
	headers: &HeaderMap,
) -> Result<(), TrafficDenied> {
	if route.allow_public_traffic || port == ENVD_PORT {
		return Ok(());
	}
	let provided = headers
		.get(TRAFFIC_TOKEN_HEADER)
		.ok_or(TrafficDenied::Missing)?;
	match &route.traffic_token {
		Some(expected) if ct_eq(provided.as_bytes(), expected.as_bytes()) => Ok(()),
		_ => Err(TrafficDenied::Invalid),
	}
}

/// Upstream `Host` for a non-envd port of a route with `maskRequestHost`
/// (`${PORT}` substituted); `None` keeps the client's Host.
pub fn masked_host(route: &SandboxRoute, port: u16) -> Option<String> {
	if port == ENVD_PORT {
		return None;
	}
	route
		.mask_request_host
		.as_deref()
		.filter(|m| !m.is_empty())
		.map(|mask| mask.replace("${PORT}", &port.to_string()))
}

#[cfg(test)]
mod tests {
	use axum::http::HeaderValue;
	use vmon_cluster::NodeId;
	use vmon_node::api::RouteState;

	use super::*;

	fn route() -> SandboxRoute {
		SandboxRoute {
			team_id:              "team".into(),
			node_id:              NodeId(1),
			template_id:          "tpl".into(),
			build_id:             "build".into(),
			state:                RouteState::Running,
			started_at_ms:        0,
			end_at_ms:            u64::MAX,
			auto_pause:           false,
			auto_resume:          false,
			allow_public_traffic: true,
			traffic_token:        None,
			mask_request_host:    None,
			https_ports:          Vec::new(),
			vcpus:                1,
			memory_mb:            512,
			envd_version:         "0.6.4".into(),
		}
	}

	#[test]
	fn control_paths_blocked_only_on_envd_port() {
		for path in [
			"/init",
			"/init/",
			"//init",
			"/a/../init",
			"/%69nit",
			"/./upgrade",
			"/fsthaw",
			"/collapse",
		] {
			assert!(is_blocked_control_path(ENVD_PORT, path), "{path}");
		}
		for path in
			["/", "/health", "/files", "/initialize", "/Init", "/init/x", "/process.Process/Start"]
		{
			assert!(!is_blocked_control_path(ENVD_PORT, path), "{path}");
		}
		assert!(!is_blocked_control_path(8080, "/init"));
	}

	#[test]
	fn traffic_token_enforcement() {
		let mut private = route();
		private.allow_public_traffic = false;
		private.traffic_token = Some("secret".into());
		let mut good = HeaderMap::new();
		good.insert(TRAFFIC_TOKEN_HEADER, HeaderValue::from_static("secret"));
		let mut bad = HeaderMap::new();
		bad.insert(TRAFFIC_TOKEN_HEADER, HeaderValue::from_static("secreT"));
		let none = HeaderMap::new();

		assert_eq!(check_traffic_token(&private, 8080, &good), Ok(()));
		assert_eq!(check_traffic_token(&private, 8080, &bad), Err(TrafficDenied::Invalid));
		assert_eq!(check_traffic_token(&private, 8080, &none), Err(TrafficDenied::Missing));
		assert_eq!(check_traffic_token(&private, ENVD_PORT, &none), Ok(()), "envd port is exempt");
		assert_eq!(check_traffic_token(&route(), 8080, &none), Ok(()), "public sandbox");

		private.traffic_token = None;
		assert_eq!(
			check_traffic_token(&private, 8080, &good),
			Err(TrafficDenied::Invalid),
			"fails closed"
		);
	}

	#[test]
	fn mask_host_substitution() {
		let mut masked = route();
		masked.mask_request_host = Some("localhost:${PORT}".into());
		assert_eq!(masked_host(&masked, 3000).as_deref(), Some("localhost:3000"));
		assert_eq!(masked_host(&masked, ENVD_PORT), None, "envd keeps Host");
		masked.mask_request_host = Some("app.internal".into());
		assert_eq!(masked_host(&masked, 3000).as_deref(), Some("app.internal"));
		masked.mask_request_host = Some(String::new());
		assert_eq!(masked_host(&masked, 3000), None);
		assert_eq!(masked_host(&route(), 3000), None);
	}
}
