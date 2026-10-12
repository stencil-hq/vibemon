//! SOCKS5 client (RFC 1928, RFC 1929 auth) for the sandbox `egressProxy`
//! upstream, optionally over TLS to the proxy.

use std::{
	io,
	net::{IpAddr, SocketAddr},
	sync::Arc,
	time::Duration,
};

use rustls::{ClientConfig, RootCertStore, pki_types::ServerName};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
	net::TcpStream,
};
use tokio_rustls::TlsConnector;

use super::{BoxedIo, policy};
use crate::api::EgressProxy;

const VERSION: u8 = 0x05;
const METHOD_NONE: u8 = 0x00;
const METHOD_USERPASS: u8 = 0x02;
const METHOD_UNACCEPTABLE: u8 = 0xff;
const CMD_CONNECT: u8 = 0x01;
const ATYP_V4: u8 = 0x01;
const ATYP_DOMAIN: u8 = 0x03;
const ATYP_V6: u8 = 0x04;

/// Destination requested from the proxy.
#[derive(Clone, Copy, Debug)]
pub enum Target<'a> {
	/// ATYP IPv4/IPv6 (CIDR-matched or default-allowed flows).
	Addr(SocketAddr),
	/// ATYP domain: the proxy resolves (domain-matched flows).
	Domain(&'a str, u16),
}

/// Run the SOCKS5 greeting, optional username/password auth, and CONNECT
/// over an established stream to the proxy.
pub async fn handshake<S: AsyncRead + AsyncWrite + Unpin>(
	stream: &mut S,
	target: Target<'_>,
	credentials: Option<(&str, &str)>,
) -> io::Result<()> {
	let method = if credentials.is_some() {
		METHOD_USERPASS
	} else {
		METHOD_NONE
	};
	stream.write_all(&[VERSION, 1, method]).await?;
	let mut reply = [0u8; 2];
	stream.read_exact(&mut reply).await?;
	if reply[0] != VERSION {
		return Err(protocol("proxy is not SOCKS5"));
	}
	match reply[1] {
		m if m == method => {},
		METHOD_UNACCEPTABLE => return Err(protocol("proxy accepted no offered auth method")),
		other => return Err(protocol(&format!("proxy chose unoffered auth method {other:#x}"))),
	}
	if let Some((user, pass)) = credentials {
		let (user, pass) = (user.as_bytes(), pass.as_bytes());
		let (Ok(user_len), Ok(pass_len)) = (u8::try_from(user.len()), u8::try_from(pass.len()))
		else {
			return Err(protocol("SOCKS5 credentials exceed 255 bytes"));
		};
		let mut msg = Vec::with_capacity(3 + user.len() + pass.len());
		msg.extend_from_slice(&[0x01, user_len]);
		msg.extend_from_slice(user);
		msg.push(pass_len);
		msg.extend_from_slice(pass);
		stream.write_all(&msg).await?;
		stream.read_exact(&mut reply).await?;
		if reply[1] != 0 {
			return Err(io::Error::new(
				io::ErrorKind::PermissionDenied,
				"SOCKS5 authentication failed",
			));
		}
	}
	let mut req = vec![VERSION, CMD_CONNECT, 0];
	match target {
		Target::Addr(SocketAddr::V4(addr)) => {
			req.push(ATYP_V4);
			req.extend_from_slice(&addr.ip().octets());
			req.extend_from_slice(&addr.port().to_be_bytes());
		},
		Target::Addr(SocketAddr::V6(addr)) => {
			req.push(ATYP_V6);
			req.extend_from_slice(&addr.ip().octets());
			req.extend_from_slice(&addr.port().to_be_bytes());
		},
		Target::Domain(host, port) => {
			let len =
				u8::try_from(host.len()).map_err(|_| protocol("SOCKS5 domain exceeds 255 bytes"))?;
			req.extend_from_slice(&[ATYP_DOMAIN, len]);
			req.extend_from_slice(host.as_bytes());
			req.extend_from_slice(&port.to_be_bytes());
		},
	}
	stream.write_all(&req).await?;
	let mut head = [0u8; 4];
	stream.read_exact(&mut head).await?;
	if head[0] != VERSION {
		return Err(protocol("malformed SOCKS5 reply"));
	}
	if head[1] != 0 {
		return Err(io::Error::new(
			io::ErrorKind::ConnectionRefused,
			format!("SOCKS5 connect failed: {}", reply_text(head[1])),
		));
	}
	let bound = match head[3] {
		ATYP_V4 => 4,
		ATYP_V6 => 16,
		ATYP_DOMAIN => usize::from(stream.read_u8().await?),
		other => return Err(protocol(&format!("SOCKS5 reply has unknown ATYP {other:#x}"))),
	};
	let mut skip = vec![0u8; bound + 2];
	stream.read_exact(&mut skip).await?;
	Ok(())
}

const fn reply_text(code: u8) -> &'static str {
	match code {
		1 => "general failure",
		2 => "connection not allowed by ruleset",
		3 => "network unreachable",
		4 => "host unreachable",
		5 => "connection refused",
		6 => "TTL expired",
		7 => "command not supported",
		8 => "address type not supported",
		_ => "unknown error",
	}
}

fn protocol(msg: &str) -> io::Error {
	io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// Connect through `proxy` (TCP or TLS per `proxy.tls`) to `target`.
/// `system_roots` back TLS verification when the policy has no `caCert`.
pub async fn connect(
	proxy: &EgressProxy,
	target: Target<'_>,
	system_roots: &Arc<RootCertStore>,
	timeout: Duration,
) -> io::Result<BoxedIo> {
	let (host, port) = policy::split_host_port(&proxy.address)
		.ok_or_else(|| protocol("invalid egressProxy.address"))?;
	let tcp = tokio::time::timeout(timeout, TcpStream::connect((host, port)))
		.await
		.map_err(|_| {
			io::Error::new(io::ErrorKind::TimedOut, "connecting to SOCKS5 proxy timed out")
		})??;
	let _ = tcp.set_nodelay(true);
	let credentials = proxy
		.username
		.as_deref()
		.map(|user| (user, proxy.password.as_deref().unwrap_or_default()));
	let mut stream: BoxedIo = match proxy.tls.as_ref().filter(|tls| tls.enabled) {
		None => Box::new(tcp),
		Some(tls) => {
			let roots = match &tls.ca_cert {
				Some(pem) => {
					let mut store = RootCertStore::empty();
					for cert in policy::parse_pem_certs(pem).map_err(|e| protocol(&e))? {
						store.add(cert).map_err(|e| protocol(&e.to_string()))?;
					}
					Arc::new(store)
				},
				None => system_roots.clone(),
			};
			let name = tls.server_name.as_deref().unwrap_or(host);
			let server_name = match name.parse::<IpAddr>() {
				Ok(ip) => ServerName::IpAddress(ip.into()),
				Err(_) => {
					ServerName::try_from(name.to_string()).map_err(|e| protocol(&e.to_string()))?
				},
			};
			let config =
				ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
					.with_safe_default_protocol_versions()
					.map_err(|e| protocol(&e.to_string()))?
					.with_root_certificates(roots)
					.with_no_client_auth();
			Box::new(
				TlsConnector::from(Arc::new(config))
					.connect(server_name, tcp)
					.await?,
			)
		},
	};
	tokio::time::timeout(timeout, handshake(&mut stream, target, credentials))
		.await
		.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "SOCKS5 handshake timed out"))??;
	Ok(stream)
}

#[cfg(test)]
pub(crate) mod tests {
	use tokio::net::TcpListener;

	use super::*;

	/// What the in-test SOCKS5 server observed.
	#[derive(Debug, PartialEq, Eq)]
	pub(in crate::egress) struct Seen {
		pub(in crate::egress) credentials: Option<(String, String)>,
		pub(in crate::egress) atyp:        u8,
		pub(in crate::egress) host:        String,
		pub(in crate::egress) port:        u16,
	}

	/// Serve one SOCKS5 CONNECT: check credentials when `require` is set,
	/// connect to `forward` (ignoring the requested target), splice, report.
	pub(in crate::egress) async fn serve_one(
		listener: TcpListener,
		require: Option<(&'static str, &'static str)>,
		forward: SocketAddr,
	) -> io::Result<Seen> {
		let (mut s, _) = listener.accept().await?;
		let mut greet = [0u8; 2];
		s.read_exact(&mut greet).await?;
		let mut methods = vec![0u8; usize::from(greet[1])];
		s.read_exact(&mut methods).await?;
		let wanted = if require.is_some() {
			METHOD_USERPASS
		} else {
			METHOD_NONE
		};
		if !methods.contains(&wanted) {
			s.write_all(&[VERSION, METHOD_UNACCEPTABLE]).await?;
			return Err(protocol("no acceptable method"));
		}
		s.write_all(&[VERSION, wanted]).await?;
		let mut credentials = None;
		if let Some((user, pass)) = require {
			let mut ver_len = [0u8; 2];
			s.read_exact(&mut ver_len).await?;
			let mut u = vec![0u8; usize::from(ver_len[1])];
			s.read_exact(&mut u).await?;
			let mut p = vec![0u8; usize::from(s.read_u8().await?)];
			s.read_exact(&mut p).await?;
			let ok = u == user.as_bytes() && p == pass.as_bytes();
			s.write_all(&[1, u8::from(!ok)]).await?;
			credentials = Some((
				String::from_utf8_lossy(&u).into_owned(),
				String::from_utf8_lossy(&p).into_owned(),
			));
			if !ok {
				return Ok(Seen { credentials, atyp: 0, host: String::new(), port: 0 });
			}
		}
		let mut head = [0u8; 4];
		s.read_exact(&mut head).await?;
		let host = match head[3] {
			ATYP_V4 => {
				let mut b = [0u8; 4];
				s.read_exact(&mut b).await?;
				IpAddr::from(b).to_string()
			},
			ATYP_V6 => {
				let mut b = [0u8; 16];
				s.read_exact(&mut b).await?;
				IpAddr::from(b).to_string()
			},
			_ => {
				let mut b = vec![0u8; usize::from(s.read_u8().await?)];
				s.read_exact(&mut b).await?;
				String::from_utf8_lossy(&b).into_owned()
			},
		};
		let port = s.read_u16().await?;
		let mut upstream = TcpStream::connect(forward).await?;
		s.write_all(&[VERSION, 0, 0, ATYP_V4, 0, 0, 0, 0, 0, 0])
			.await?;
		let seen = Seen { credentials, atyp: head[3], host, port };
		tokio::spawn(async move {
			let _ = tokio::io::copy_bidirectional(&mut s, &mut upstream).await;
		});
		Ok(seen)
	}

	async fn echo_server() -> SocketAddr {
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let addr = listener.local_addr().expect("addr");
		tokio::spawn(async move {
			while let Ok((mut s, _)) = listener.accept().await {
				tokio::spawn(async move {
					let (mut r, mut w) = s.split();
					let _ = tokio::io::copy(&mut r, &mut w).await;
				});
			}
		});
		addr
	}

	fn proxy_cfg(addr: SocketAddr, user: Option<(&str, &str)>) -> EgressProxy {
		EgressProxy {
			address:  addr.to_string(),
			username: user.map(|(u, _)| u.to_string()),
			password: user.map(|(_, p)| p.to_string()),
			tls:      None,
		}
	}

	#[tokio::test]
	async fn auth_and_domain_atyp() {
		let echo = echo_server().await;
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let proxy = proxy_cfg(listener.local_addr().expect("addr"), Some(("alice", "s3cret")));
		let server = tokio::spawn(serve_one(listener, Some(("alice", "s3cret")), echo));
		let roots = Arc::new(RootCertStore::empty());
		let mut stream =
			connect(&proxy, Target::Domain("api.example.com", 443), &roots, Duration::from_secs(5))
				.await
				.expect("connect");
		stream.write_all(b"ping").await.expect("write");
		let mut buf = [0u8; 4];
		stream.read_exact(&mut buf).await.expect("read");
		assert_eq!(&buf, b"ping");
		let seen = server.await.expect("join").expect("serve");
		assert_eq!(seen, Seen {
			credentials: Some(("alice".into(), "s3cret".into())),
			atyp:        ATYP_DOMAIN,
			host:        "api.example.com".into(),
			port:        443,
		});
	}

	#[tokio::test]
	async fn ip_target_without_auth() {
		let echo = echo_server().await;
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let proxy = proxy_cfg(listener.local_addr().expect("addr"), None);
		let server = tokio::spawn(serve_one(listener, None, echo));
		let roots = Arc::new(RootCertStore::empty());
		let target: SocketAddr = "[2001:db8::7]:8080".parse().expect("addr");
		connect(&proxy, Target::Addr(target), &roots, Duration::from_secs(5))
			.await
			.expect("connect");
		let seen = server.await.expect("join").expect("serve");
		assert_eq!((seen.atyp, seen.host.as_str(), seen.port), (ATYP_V6, "2001:db8::7", 8080));
	}

	#[tokio::test]
	async fn wrong_password_is_permission_denied() {
		let echo = echo_server().await;
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let proxy = proxy_cfg(listener.local_addr().expect("addr"), Some(("alice", "wrong")));
		let server = tokio::spawn(serve_one(listener, Some(("alice", "s3cret")), echo));
		let roots = Arc::new(RootCertStore::empty());
		let err = connect(&proxy, Target::Domain("x.io", 80), &roots, Duration::from_secs(5))
			.await
			.err()
			.expect("must fail");
		assert_eq!(err.kind(), io::ErrorKind::PermissionDenied);
		server.await.expect("join").expect("serve");
	}
}
