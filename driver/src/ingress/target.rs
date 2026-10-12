//! Public request dispatch (ARCHITECTURE.md §3): decides
//! whether a request addresses a sandbox port and, if so, which one.

use axum::http::{HeaderMap, Uri, header};

/// Header carrying the sandbox id for header-based routing.
pub const SANDBOX_ID_HEADER: &str = "e2b-sandbox-id";
/// Header carrying the guest port for header-based routing.
pub const SANDBOX_PORT_HEADER: &str = "e2b-sandbox-port";

/// A sandbox port addressed by a public request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Target {
	pub sandbox_id: String,
	pub port:       u16,
}

/// Outcome of inspecting a public request.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Dispatch {
	/// Proxy to a sandbox port.
	Sandbox(Target),
	/// Routing headers present but malformed: answered with a plain-text 400.
	Invalid(&'static str),
	/// Not addressed to a sandbox: the E2B API router handles it.
	Api,
}

/// Classifies a request by Host / `:authority` first, then by the
/// `E2b-Sandbox-*` headers; called by the ingress middleware for every request.
pub fn dispatch(uri: &Uri, headers: &HeaderMap) -> Dispatch {
	if let Some(host) = request_host(uri, headers)
		&& let Some(target) = parse_host(&host)
	{
		return Dispatch::Sandbox(target);
	}
	let id = headers.get(SANDBOX_ID_HEADER).and_then(|v| v.to_str().ok());
	let port = headers
		.get(SANDBOX_PORT_HEADER)
		.and_then(|v| v.to_str().ok());
	match (id, port) {
		(None, None) => Dispatch::Api,
		(Some(_), None) | (None, Some(_)) => Dispatch::Invalid("missing header"),
		(Some(id), Some(port)) => {
			let Ok(port) = port.trim().parse::<u16>() else {
				return Dispatch::Invalid("Invalid sandbox port");
			};
			let id = id.trim();
			if !valid_sandbox_id(id) {
				return Dispatch::Invalid("Invalid sandbox ID");
			}
			Dispatch::Sandbox(Target { sandbox_id: id.to_owned(), port })
		},
	}
}

/// Host the client addressed: the `Host` header (HTTP/1.1) or the URI
/// authority (HTTP/2 `:authority`), lowercased and without port.
pub fn request_host(uri: &Uri, headers: &HeaderMap) -> Option<String> {
	let raw = request_authority(uri, headers)?;
	Some(strip_port(&raw).to_ascii_lowercase())
}

/// Full `host[:port]` as sent by the client (for `X-Forwarded-Host` and
/// the upstream `Host`).
pub fn request_authority(uri: &Uri, headers: &HeaderMap) -> Option<String> {
	headers
		.get(header::HOST)
		.and_then(|v| v.to_str().ok())
		.map(str::to_owned)
		.or_else(|| uri.authority().map(|a| a.as_str().to_owned()))
}

/// Host part of an authority (IPv6 brackets removed).
pub fn strip_port(authority: &str) -> &str {
	if let Some(rest) = authority.strip_prefix('[') {
		return rest.split(']').next().unwrap_or(rest);
	}
	authority
		.rsplit_once(':')
		.map_or(authority, |(host, port)| {
			if port.bytes().all(|b| b.is_ascii_digit()) {
				host
			} else {
				authority
			}
		})
}

/// `<port>-<sandboxID>[-ignored…].<anything>` → target. Hosts without a dot,
/// IP literals, or labels that do not fit fall through to the API.
fn parse_host(host: &str) -> Option<Target> {
	if host.parse::<std::net::IpAddr>().is_ok() {
		return None;
	}
	let (label, _) = host.split_once('.')?;
	let mut parts = label.split('-');
	let port = parts.next()?;
	let id = parts.next()?;
	if port.is_empty() || !port.bytes().all(|b| b.is_ascii_digit()) {
		return None;
	}
	let port = port.parse::<u16>().ok()?;
	valid_sandbox_id(id).then(|| Target { sandbox_id: id.to_owned(), port })
}

fn valid_sandbox_id(id: &str) -> bool {
	!id.is_empty()
		&& id
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
}

#[cfg(test)]
mod tests {
	use axum::http::HeaderValue;

	use super::*;

	fn run(uri: &str, host: Option<&str>, hdrs: &[(&str, &str)]) -> Dispatch {
		let uri: Uri = uri.parse().unwrap();
		let mut headers = HeaderMap::new();
		if let Some(host) = host {
			headers.insert(header::HOST, HeaderValue::from_str(host).unwrap());
		}
		for (k, v) in hdrs {
			headers.insert(
				header::HeaderName::from_bytes(k.as_bytes()).unwrap(),
				HeaderValue::from_str(v).unwrap(),
			);
		}
		dispatch(&uri, &headers)
	}

	fn sbx(id: &str, port: u16) -> Dispatch {
		Dispatch::Sandbox(Target { sandbox_id: id.into(), port })
	}

	#[test]
	fn host_routing_table() {
		let cases: &[(&str, Dispatch)] = &[
			("49983-iabc123.e2b.local", sbx("iabc123", 49983)),
			("49983-iabc123.e2b.local:3000", sbx("iabc123", 49983)),
			("8080-iabc123-6532622b.other.domain", sbx("iabc123", 8080)),
			("49983-IABC.e2b.local", sbx("iabc", 49983)),
			("49983-iabc.localhost", sbx("iabc", 49983)),
			("api.e2b.local", Dispatch::Api),
			("localhost", Dispatch::Api),
			("localhost:3000", Dispatch::Api),
			("127.0.0.1:3000", Dispatch::Api),
			("[::1]:3000", Dispatch::Api),
			("sandbox.e2b.local", Dispatch::Api),
			("my-api.example.com", Dispatch::Api),
			("99999-iabc.e2b.local", Dispatch::Api),
			("-iabc.e2b.local", Dispatch::Api),
			("49983-.e2b.local", Dispatch::Api),
			("49983.e2b.local", Dispatch::Api),
			("49983-iabc", Dispatch::Api),
			("49983-ia_bc.e2b.local", Dispatch::Api),
		];
		for (host, want) in cases {
			assert_eq!(&run("/", Some(host), &[]), want, "host {host}");
		}
	}

	#[test]
	fn http2_authority_is_used_without_host_header() {
		assert_eq!(run("https://49999-iabc.e2b.local/execute", None, &[]), sbx("iabc", 49999));
		assert_eq!(run("https://api.e2b.local/sandboxes", None, &[]), Dispatch::Api);
	}

	#[test]
	fn header_routing_table() {
		let both = [(SANDBOX_ID_HEADER, "iabc"), (SANDBOX_PORT_HEADER, "49983")];
		for host in
			["localhost:3000", "127.0.0.1", "[::1]:8080", "sandbox.e2b.local", "api.e2b.local"]
		{
			assert_eq!(run("/health", Some(host), &both), sbx("iabc", 49983), "host {host}");
		}
		assert_eq!(
			run("/", Some("localhost"), &[(SANDBOX_ID_HEADER, "iabc")]),
			Dispatch::Invalid("missing header")
		);
		assert_eq!(
			run("/", Some("localhost"), &[(SANDBOX_PORT_HEADER, "1")]),
			Dispatch::Invalid("missing header")
		);
		assert_eq!(
			run("/", Some("localhost"), &[
				(SANDBOX_ID_HEADER, "iabc"),
				(SANDBOX_PORT_HEADER, "70000")
			]),
			Dispatch::Invalid("Invalid sandbox port")
		);
		assert_eq!(
			run("/", Some("localhost"), &[(SANDBOX_ID_HEADER, "i-abc"), (SANDBOX_PORT_HEADER, "1")]),
			Dispatch::Invalid("Invalid sandbox ID")
		);
	}

	#[test]
	fn host_target_wins_over_headers() {
		let hdrs = [(SANDBOX_ID_HEADER, "iother"), (SANDBOX_PORT_HEADER, "1")];
		assert_eq!(run("/", Some("6080-iabc.e2b.local"), &hdrs), sbx("iabc", 6080));
	}
}
