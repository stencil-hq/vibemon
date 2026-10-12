//! envd authorization: `X-Access-Token` enforcement and its exempt routes,
//! `/files` URL signatures (`v1_` + base64(sha256(path:op:user:token[:exp]))),
//! Basic-auth usernames, and the loopback-root peer check that lets the
//! host's GC4 tunnel rotate the token on `/init` (our stand-in for MMDS).

use std::net::{IpAddr, SocketAddr};

use base64::Engine;
use http::{HeaderMap, Method};
use sha2::{Digest, Sha256};

/// Body of the 401 envd returns for a missing/invalid token.
pub const UNAUTHORIZED_MESSAGE: &str =
	"unauthorized access, please provide a valid access token or method signing if supported";

/// Routes reachable without `X-Access-Token` even when a token is set.
pub fn is_exempt(method: &Method, path: &str) -> bool {
	matches!(
		(method, path),
		(&Method::GET, "/health")
			| (&Method::GET | &Method::POST, "/files")
			| (&Method::POST, "/init")
	)
}

/// Constant-time equality.
pub fn ct_eq(a: &[u8], b: &[u8]) -> bool {
	if a.len() != b.len() {
		return false;
	}
	a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// True when the request carries the configured token (or none is set).
pub fn token_ok(headers: &HeaderMap, token: Option<&str>) -> bool {
	let Some(token) = token else {
		return true;
	};
	headers
		.get("x-access-token")
		.is_some_and(|v| ct_eq(v.as_bytes(), token.as_bytes()))
}

/// File transfer direction a signature authorizes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FileOp {
	Read,
	Write,
}

impl FileOp {
	const fn as_str(self) -> &'static str {
		match self {
			Self::Read => "read",
			Self::Write => "write",
		}
	}
}

/// Compute a `/files` signature exactly as the SDKs do.
pub fn file_signature(
	path: &str,
	op: FileOp,
	user: &str,
	token: &str,
	expiration: Option<i64>,
) -> String {
	let mut raw = format!("{path}:{}:{user}:{token}", op.as_str());
	if let Some(exp) = expiration {
		raw.push(':');
		raw.push_str(&exp.to_string());
	}
	let digest = Sha256::digest(raw.as_bytes());
	format!("v1_{}", base64::engine::general_purpose::STANDARD_NO_PAD.encode(digest))
}

/// Raw (`path`, `username`, `signature`, `signature_expiration`) query values.
#[derive(Debug, Default)]
pub struct SignedQuery<'a> {
	pub path:       Option<&'a str>,
	pub username:   Option<&'a str>,
	pub signature:  Option<&'a str>,
	pub expiration: Option<&'a str>,
}

/// Authorize a `/files` request (envd `validateSigning`): open when no token
/// is set; a header must match exactly; otherwise a valid, unexpired
/// signature over the raw query values is required. `Err` = 401 message.
pub fn validate_file_access(
	headers: &HeaderMap,
	token: Option<&str>,
	op: FileOp,
	query: &SignedQuery<'_>,
	now_unix: i64,
) -> Result<(), String> {
	let Some(token) = token else {
		return Ok(());
	};
	if headers.contains_key("x-access-token") {
		return if token_ok(headers, Some(token)) {
			Ok(())
		} else {
			Err("access token present in header but does not match".into())
		};
	}
	let Some(signature) = query.signature else {
		return Err("missing signature query parameter".into());
	};
	let expiration = match query.expiration {
		Some(raw) => Some(
			raw.parse::<i64>()
				.map_err(|_| "invalid signature expiration".to_owned())?,
		),
		None => None,
	};
	let expected =
		file_signature(query.path.unwrap_or(""), op, query.username.unwrap_or(""), token, expiration);
	if !ct_eq(expected.as_bytes(), signature.as_bytes()) {
		return Err("invalid signature".into());
	}
	if let Some(exp) = expiration
		&& exp < now_unix
	{
		return Err("signature is already expired".into());
	}
	Ok(())
}

/// Username from `Authorization: Basic base64(user:pass)`. `Ok(None)` when
/// the header is absent or not Basic; `Err` when Basic but malformed.
pub fn basic_auth_user(headers: &HeaderMap) -> Result<Option<String>, String> {
	let Some(value) = headers.get(http::header::AUTHORIZATION) else {
		return Ok(None);
	};
	let value = value
		.to_str()
		.map_err(|_| "invalid authorization header".to_owned())?;
	let Some((scheme, cred)) = value.split_once(' ') else {
		return Ok(None);
	};
	if !scheme.eq_ignore_ascii_case("basic") {
		return Ok(None);
	}
	let decoded = base64::engine::general_purpose::STANDARD
		.decode(cred.trim())
		.map_err(|_| "invalid basic auth encoding".to_owned())?;
	let decoded =
		String::from_utf8(decoded).map_err(|_| "invalid basic auth encoding".to_owned())?;
	let user = decoded.split_once(':').map_or(decoded.as_str(), |(u, _)| u);
	Ok(Some(user.to_owned()))
}

/// True when `peer` is a loopback connection whose socket (the client end)
/// is owned by uid 0 — the agent's GC4 tunnel or an in-guest root process.
/// Looked up in `/proc/net/tcp{,6}` by matching the 4-tuple.
pub fn peer_is_local_root(peer: SocketAddr, local: SocketAddr) -> bool {
	if !peer.ip().is_loopback() && !is_mapped_loopback(peer.ip()) {
		return false;
	}
	["/proc/net/tcp", "/proc/net/tcp6"].iter().any(|table| {
		std::fs::read_to_string(table).is_ok_and(|text| socket_uid(&text, peer, local) == Some(0))
	})
}

fn is_mapped_loopback(ip: IpAddr) -> bool {
	matches!(ip, IpAddr::V6(v6) if v6.to_ipv4_mapped().is_some_and(|v4| v4.is_loopback()))
}

/// uid of the socket in a `/proc/net/tcp*` table whose local address is
/// `owner_side` and remote address is `other_side`.
fn socket_uid(table: &str, owner_side: SocketAddr, other_side: SocketAddr) -> Option<u32> {
	table.lines().skip(1).find_map(|line| {
		let cols: Vec<&str> = line.split_whitespace().collect();
		if cols.len() < 8 {
			return None;
		}
		let local = parse_proc_addr(cols[1])?;
		let remote = parse_proc_addr(cols[2])?;
		(same_endpoint(local, owner_side) && same_endpoint(remote, other_side))
			.then(|| cols[7].parse().ok())?
	})
}

fn same_endpoint(a: SocketAddr, b: SocketAddr) -> bool {
	let canon = |ip: IpAddr| match ip {
		IpAddr::V6(v6) => v6.to_ipv4_mapped().map_or(IpAddr::V6(v6), IpAddr::V4),
		v4 @ IpAddr::V4(_) => v4,
	};
	a.port() == b.port() && canon(a.ip()) == canon(b.ip())
}

/// Parse `0100007F:C35F` / 32-hex-digit IPv6 forms (host-endian u32 words).
fn parse_proc_addr(s: &str) -> Option<SocketAddr> {
	let (ip_hex, port_hex) = s.split_once(':')?;
	let port = u16::from_str_radix(port_hex, 16).ok()?;
	let ip = match ip_hex.len() {
		8 => IpAddr::V4(u32::from_str_radix(ip_hex, 16).ok()?.to_ne_bytes().into()),
		32 => {
			let mut bytes = [0u8; 16];
			for (i, chunk) in bytes.chunks_mut(4).enumerate() {
				let word = u32::from_str_radix(ip_hex.get(i * 8..i * 8 + 8)?, 16).ok()?;
				chunk.copy_from_slice(&word.to_ne_bytes());
			}
			IpAddr::V6(bytes.into())
		},
		_ => return None,
	};
	Some(SocketAddr::new(ip, port))
}

#[cfg(test)]
mod tests {
	use super::*;

	fn headers(pairs: &[(&'static str, &str)]) -> HeaderMap {
		let mut h = HeaderMap::new();
		for (k, v) in pairs {
			h.insert(*k, v.parse().unwrap());
		}
		h
	}

	#[test]
	fn signature_matches_sdk_algorithm() {
		// sha256("/home/user/a.txt:read:user:tok") in unpadded std base64.
		let sig = file_signature("/home/user/a.txt", FileOp::Read, "user", "tok", None);
		let digest = Sha256::digest(b"/home/user/a.txt:read:user:tok");
		let expected =
			format!("v1_{}", base64::engine::general_purpose::STANDARD_NO_PAD.encode(digest));
		assert_eq!(sig, expected);
		assert!(!sig.ends_with('='));
		let with_exp = file_signature("p", FileOp::Write, "", "tok", Some(1_700_000_000));
		let digest = Sha256::digest(b"p:write::tok:1700000000");
		assert_eq!(
			with_exp,
			format!("v1_{}", base64::engine::general_purpose::STANDARD_NO_PAD.encode(digest))
		);
	}

	#[test]
	fn file_access_rules() {
		let none = HeaderMap::new();
		let q = SignedQuery { path: Some("/f"), username: Some("user"), ..Default::default() };
		// No token configured: always open.
		assert!(validate_file_access(&none, None, FileOp::Read, &q, 0).is_ok());
		// Token set, no header, no signature.
		assert!(validate_file_access(&none, Some("t"), FileOp::Read, &q, 0).is_err());
		// Header wins (signature not consulted), but must match.
		assert!(
			validate_file_access(&headers(&[("x-access-token", "t")]), Some("t"), FileOp::Read, &q, 0)
				.is_ok()
		);
		assert!(
			validate_file_access(&headers(&[("x-access-token", "x")]), Some("t"), FileOp::Read, &q, 0)
				.is_err()
		);
		// Valid signature for read is not valid for write.
		let sig = file_signature("/f", FileOp::Read, "user", "t", None);
		let q = SignedQuery { signature: Some(&sig), ..q };
		assert!(validate_file_access(&none, Some("t"), FileOp::Read, &q, 0).is_ok());
		assert!(validate_file_access(&none, Some("t"), FileOp::Write, &q, 0).is_err());
		// Expiration participates in the hash and is enforced (>= now).
		let sig = file_signature("/f", FileOp::Read, "user", "t", Some(100));
		let q = SignedQuery { signature: Some(&sig), expiration: Some("100"), ..q };
		assert!(validate_file_access(&none, Some("t"), FileOp::Read, &q, 100).is_ok());
		assert_eq!(
			validate_file_access(&none, Some("t"), FileOp::Read, &q, 101).unwrap_err(),
			"signature is already expired"
		);
		// Tampered expiration breaks the signature.
		let q = SignedQuery { expiration: Some("200"), ..q };
		assert_eq!(
			validate_file_access(&none, Some("t"), FileOp::Read, &q, 0).unwrap_err(),
			"invalid signature"
		);
		// Signature over an absent path/username uses "".
		let sig = file_signature("", FileOp::Write, "", "t", None);
		let q = SignedQuery { signature: Some(&sig), ..Default::default() };
		assert!(validate_file_access(&none, Some("t"), FileOp::Write, &q, 0).is_ok());
	}

	#[test]
	fn exempt_routes() {
		assert!(is_exempt(&Method::GET, "/health"));
		assert!(is_exempt(&Method::POST, "/files"));
		assert!(is_exempt(&Method::GET, "/files"));
		assert!(is_exempt(&Method::POST, "/init"));
		assert!(!is_exempt(&Method::POST, "/health"));
		assert!(!is_exempt(&Method::POST, "/files/compose"));
		assert!(!is_exempt(&Method::GET, "/envs"));
		assert!(!is_exempt(&Method::POST, "/process.Process/Start"));
	}

	#[test]
	fn basic_auth_parsing() {
		assert_eq!(basic_auth_user(&HeaderMap::new()).unwrap(), None);
		// base64("user:")
		assert_eq!(
			basic_auth_user(&headers(&[("authorization", "Basic dXNlcjo=")]))
				.unwrap()
				.as_deref(),
			Some("user")
		);
		assert_eq!(
			basic_auth_user(&headers(&[("authorization", "basic cm9vdA==")]))
				.unwrap()
				.as_deref(),
			Some("root")
		);
		assert_eq!(basic_auth_user(&headers(&[("authorization", "Bearer abc")])).unwrap(), None);
		assert!(basic_auth_user(&headers(&[("authorization", "Basic !!!")])).is_err());
	}

	#[test]
	fn proc_net_tcp_lookup() {
		let table = "  sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   \
		             uid  timeout inode\n0: 0100007F:C35F 0100007F:9C40 01 00000000:00000000 \
		             00:00000000 00000000  1000        0 1 1\n1: 0100007F:9C40 0100007F:C35F 01 \
		             00000000:00000000 00:00000000 00000000     0        0 2 1\n";
		let envd: SocketAddr = "127.0.0.1:50015".parse().unwrap();
		let client: SocketAddr = "127.0.0.1:40000".parse().unwrap();
		// The client's socket (local=client, remote=envd) is owned by uid 0.
		assert_eq!(socket_uid(table, client, envd), Some(0));
		assert_eq!(socket_uid(table, envd, client), Some(1000));
		assert_eq!(socket_uid(table, "127.0.0.1:1".parse().unwrap(), envd), None);
		assert!(!peer_is_local_root("10.0.0.2:5".parse().unwrap(), envd));
	}
}
