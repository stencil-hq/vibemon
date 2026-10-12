//! Per-connection logic of the transparent egress proxy, independent of how
//! the connection and its original destination were obtained (Linux:
//! iptables `REDIRECT` + `SO_ORIGINAL_DST`, see `linux.rs`).

use std::{
	io,
	net::SocketAddr,
	pin::Pin,
	sync::Arc,
	task::{Context, Poll},
	time::Duration,
};

use rustls::{ClientConfig, RootCertStore};
use tokio::{
	io::{AsyncRead, AsyncReadExt, AsyncWrite, ReadBuf},
	net::TcpStream,
};

use super::{
	BoxedIo,
	mitm::{self, Ca},
	policy::{Compiled, Decision},
	sniff::{self, Sniff},
	socks,
};

/// How long the proxy waits for a Host header / `ClientHello` before deciding
/// on CIDR rules alone (server-speaks-first protocols on :80/:443).
pub const SNIFF_TIMEOUT: Duration = Duration::from_secs(5);
/// Upstream dial timeout (E2B `upstreamDialTimeout`).
pub const DIAL_TIMEOUT: Duration = Duration::from_secs(30);

/// Node-wide state shared by every proxied flow.
pub struct Shared {
	/// Interception CA; `None` disables `rules` (flows pass through).
	pub ca:            Option<Arc<Ca>>,
	/// Trust store for TLS proxies without `caCert`.
	pub roots:         Arc<RootCertStore>,
	/// TLS client config toward real origins of intercepted flows.
	pub origin_tls:    Arc<ClientConfig>,
	pub sniff_timeout: Duration,
	pub dial_timeout:  Duration,
}

impl Shared {
	/// Production settings: Mozilla roots, default timeouts.
	pub fn new(ca: Option<Arc<Ca>>) -> anyhow::Result<Self> {
		let roots = mitm::system_roots();
		Ok(Self {
			ca,
			origin_tls: mitm::upstream_client_config(roots.clone())?,
			roots,
			sniff_timeout: SNIFF_TIMEOUT,
			dial_timeout: DIAL_TIMEOUT,
		})
	}
}

/// Proxy one guest connection whose original destination was `orig`.
/// Returns after both directions finish; a denied flow is closed at once.
pub async fn handle<S>(
	mut client: S,
	orig: SocketAddr,
	policy: Arc<Compiled>,
	shared: &Shared,
) -> io::Result<()>
where
	S: AsyncRead + AsyncWrite + Send + Unpin + 'static,
{
	let mut prefix = Vec::new();
	let host = if policy.wants_hostname() && matches!(orig.port(), 80 | 443) {
		sniff_host(&mut client, &mut prefix, orig.port(), shared.sniff_timeout).await?
	} else {
		None
	};
	let decision = policy.decide(host.as_deref(), orig.ip());
	if decision == Decision::Deny {
		tracing::debug!(%orig, host = host.as_deref().unwrap_or(""), "egress denied");
		return Ok(());
	}
	let mut upstream = match dial(&policy, decision, host.as_deref(), orig, shared).await {
		Ok(upstream) => upstream,
		Err(e) => {
			// The only operator-visible trace of an allowed flow that cannot
			// be served (e.g. the node's resolver cannot resolve an allowed
			// domain, which is re-dialed by name on the host).
			tracing::warn!(%orig, host = host.as_deref().unwrap_or(""), ?decision, "egress upstream dial failed: {e}");
			return Err(e);
		},
	};
	let client = Prefixed::new(prefix, client);
	if orig.port() == 443
		&& let (Some(host), Some(ca)) = (host.as_deref(), shared.ca.as_ref())
		&& let Some(headers) = policy.rules_for(host)
	{
		let overrides = mitm::header_overrides(headers)?;
		let server = ca.server_config(host).map_err(io::Error::other)?;
		return mitm::intercept(
			Box::new(client),
			upstream,
			host,
			overrides,
			server,
			shared.origin_tls.clone(),
		)
		.await;
	}
	let mut client = client;
	tokio::io::copy_bidirectional(&mut client, &mut upstream)
		.await
		.map(|_| ())
}

/// Read from the guest until the port's parser yields a verdict, EOF, the
/// size cap, or the timeout. Everything read is left in `prefix`.
async fn sniff_host<S: AsyncRead + Unpin>(
	client: &mut S,
	prefix: &mut Vec<u8>,
	port: u16,
	timeout: Duration,
) -> io::Result<Option<String>> {
	let parse = if port == 443 {
		sniff::tls_sni
	} else {
		sniff::http_host
	};
	let deadline = tokio::time::Instant::now() + timeout;
	let mut chunk = [0u8; 4096];
	loop {
		let read = match tokio::time::timeout_at(deadline, client.read(&mut chunk)).await {
			Err(_) => return Ok(None),
			Ok(read) => read?,
		};
		if read == 0 {
			return Ok(None);
		}
		prefix.extend_from_slice(&chunk[..read]);
		match parse(prefix) {
			Sniff::NeedMore if prefix.len() < sniff::MAX_SNIFF_BYTES => {},
			Sniff::Host(host) => return Ok(Some(host)),
			Sniff::NeedMore | Sniff::NoHost => return Ok(None),
		}
	}
}

async fn dial(
	policy: &Compiled,
	decision: Decision,
	host: Option<&str>,
	orig: SocketAddr,
	shared: &Shared,
) -> io::Result<BoxedIo> {
	let by_name = (decision == Decision::Domain).then_some(host).flatten();
	if let Some(proxy) = policy.egress_proxy() {
		let target = match by_name {
			Some(host) => socks::Target::Domain(host, orig.port()),
			None => socks::Target::Addr(orig),
		};
		return socks::connect(proxy, target, &shared.roots, shared.dial_timeout).await;
	}
	let stream = match by_name {
		Some(host) => dial_name(policy, host, orig.port(), shared.dial_timeout).await?,
		None => connect_timeout(orig, shared.dial_timeout).await?,
	};
	Ok(Box::new(stream))
}

/// Resolve on the host and connect to the first permitted address; an
/// address in a private range is never dialed (E2B `proxyWithIPVerification`).
async fn dial_name(
	policy: &Compiled,
	host: &str,
	port: u16,
	timeout: Duration,
) -> io::Result<TcpStream> {
	let addrs = tokio::time::timeout(timeout, tokio::net::lookup_host((host, port)))
		.await
		.map_err(|_| {
			io::Error::new(io::ErrorKind::TimedOut, "resolving egress hostname timed out")
		})??;
	let mut last = io::Error::new(
		io::ErrorKind::PermissionDenied,
		format!("{host} resolved only to blocked addresses"),
	);
	for addr in addrs {
		if !policy.resolved_ip_allowed(addr.ip()) {
			tracing::debug!(%addr, host, "egress hostname resolved to a blocked address");
			continue;
		}
		match connect_timeout(addr, timeout).await {
			Ok(stream) => return Ok(stream),
			Err(e) => last = e,
		}
	}
	Err(last)
}

async fn connect_timeout(addr: SocketAddr, timeout: Duration) -> io::Result<TcpStream> {
	let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
		.await
		.map_err(|_| {
			io::Error::new(io::ErrorKind::TimedOut, format!("connecting to {addr} timed out"))
		})??;
	let _ = stream.set_nodelay(true);
	Ok(stream)
}

/// A stream that first replays bytes already consumed while sniffing.
pub struct Prefixed<S> {
	prefix: Vec<u8>,
	pos:    usize,
	inner:  S,
}

impl<S> Prefixed<S> {
	/// Replay `prefix` before reading from `inner`.
	pub const fn new(prefix: Vec<u8>, inner: S) -> Self {
		Self { prefix, pos: 0, inner }
	}
}

impl<S: AsyncRead + Unpin> AsyncRead for Prefixed<S> {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<io::Result<()>> {
		if self.pos < self.prefix.len() {
			let n = buf.remaining().min(self.prefix.len() - self.pos);
			let start = self.pos;
			buf.put_slice(&self.prefix[start..start + n]);
			self.pos += n;
			if self.pos == self.prefix.len() {
				self.prefix = Vec::new();
				self.pos = 0;
			}
			return Poll::Ready(Ok(()));
		}
		Pin::new(&mut self.inner).poll_read(cx, buf)
	}
}

impl<S: AsyncWrite + Unpin> AsyncWrite for Prefixed<S> {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &[u8],
	) -> Poll<io::Result<usize>> {
		Pin::new(&mut self.inner).poll_write(cx, buf)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.inner).poll_flush(cx)
	}

	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
		Pin::new(&mut self.inner).poll_shutdown(cx)
	}
}

#[cfg(test)]
mod tests {
	use std::collections::BTreeMap;

	use tokio::{io::AsyncWriteExt, net::TcpListener};

	use super::*;
	use crate::api::{EgressProxy, NetworkPolicy, NetworkRule};

	fn compile(policy: &NetworkPolicy, internal: &[&str]) -> Arc<Compiled> {
		let internal: Vec<_> = internal.iter().map(|c| c.parse().expect("cidr")).collect();
		Arc::new(Compiled::compile(policy, &internal).expect("policy"))
	}

	/// Run `handle` for one accepted connection with a forced original dst.
	async fn proxy_once(policy: Arc<Compiled>, shared: Arc<Shared>, orig: SocketAddr) -> SocketAddr {
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let addr = listener.local_addr().expect("addr");
		tokio::spawn(async move {
			let (guest, _) = listener.accept().await.expect("accept");
			let _ = handle(guest, orig, policy, &shared).await;
		});
		addr
	}

	async fn http_origin() -> SocketAddr {
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let addr = listener.local_addr().expect("addr");
		tokio::spawn(async move {
			while let Ok((mut s, _)) = listener.accept().await {
				tokio::spawn(async move {
					let mut buf = vec![0u8; 4096];
					let n = s.read(&mut buf).await.unwrap_or(0);
					let head = String::from_utf8_lossy(&buf[..n]).into_owned();
					let _ = s
						.write_all(format!("HTTP/1.0 200 OK\r\n\r\n{head}").as_bytes())
						.await;
				});
			}
		});
		addr
	}

	fn test_shared(ca: Option<Arc<Ca>>, origin_roots: Arc<RootCertStore>) -> Arc<Shared> {
		Arc::new(Shared {
			ca,
			origin_tls: mitm::upstream_client_config(origin_roots.clone()).expect("tls"),
			roots: origin_roots,
			sniff_timeout: Duration::from_millis(300),
			dial_timeout: Duration::from_secs(5),
		})
	}

	async fn roundtrip(proxy: SocketAddr, request: &[u8]) -> String {
		let mut s = TcpStream::connect(proxy).await.expect("connect");
		s.write_all(request).await.expect("write");
		let mut out = Vec::new();
		let _ = s.read_to_end(&mut out).await;
		String::from_utf8_lossy(&out).into_owned()
	}

	/// Regression: an OpenSSL 3.5 `ClientHello` (≈1.7 KB with an MLKEM key
	/// share) arrives in two TCP segments; the SNI after the key share must
	/// still be found and every byte replayed upstream.
	#[tokio::test]
	async fn sni_spanning_two_segments_is_sniffed_and_replayed() {
		let hello =
			crate::egress::sniff::tests::client_hello_with_key_share(Some("example.com"), 1500);
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let addr = listener.local_addr().expect("addr");
		let sent = hello.clone();
		let writer = tokio::spawn(async move {
			let mut s = TcpStream::connect(addr).await.expect("connect");
			s.write_all(&sent[..1000]).await.expect("first segment");
			s.flush().await.expect("flush");
			tokio::time::sleep(Duration::from_millis(50)).await;
			s.write_all(&sent[1000..]).await.expect("second segment");
			s
		});
		let (mut guest, _) = listener.accept().await.expect("accept");
		let mut prefix = Vec::new();
		let host = sniff_host(&mut guest, &mut prefix, 443, Duration::from_secs(5))
			.await
			.expect("sniff");
		assert_eq!(host.as_deref(), Some("example.com"));
		let _client = writer.await.expect("writer");
		let mut replayed = vec![0u8; hello.len()];
		Prefixed::new(prefix, guest)
			.read_exact(&mut replayed)
			.await
			.expect("replay");
		assert_eq!(replayed, hello);
	}
	#[tokio::test]
	async fn exempt_cidr_passes_and_prefix_is_replayed() {
		let origin = http_origin().await;
		// 127/8 is always denied unless the node exempts it.
		let denied = proxy_once(
			compile(&NetworkPolicy::default(), &[]),
			test_shared(None, Arc::new(RootCertStore::empty())),
			origin,
		)
		.await;
		assert_eq!(roundtrip(denied, b"GET / HTTP/1.1\r\nHost: a.io\r\n\r\n").await, "");
		let allowed = proxy_once(
			compile(&NetworkPolicy::default(), &["127.0.0.0/8"]),
			test_shared(None, Arc::new(RootCertStore::empty())),
			origin,
		)
		.await;
		let out = roundtrip(allowed, b"GET / HTTP/1.1\r\nHost: a.io\r\n\r\n").await;
		assert!(out.contains("Host: a.io"), "{out}");
	}

	#[tokio::test]
	async fn user_deny_applies_and_unknown_host_falls_back_to_cidr() {
		let origin = http_origin().await;
		let policy = NetworkPolicy {
			allow_out: vec!["allowed.example".into()],
			deny_out: vec!["0.0.0.0/0".into()],
			..NetworkPolicy::default()
		};
		// Wrong Host on :80-style sniffing is evaluated on the CIDR (denied).
		let orig80 = SocketAddr::new(origin.ip(), 80);
		let p = proxy_once(
			compile(&policy, &["127.0.0.0/8"]),
			test_shared(None, Arc::new(RootCertStore::empty())),
			orig80,
		)
		.await;
		assert_eq!(roundtrip(p, b"GET / HTTP/1.1\r\nHost: other.example\r\n\r\n").await, "");
		// A non-HTTP client on port 80 times out sniffing and is denied too.
		let p = proxy_once(
			compile(&policy, &["127.0.0.0/8"]),
			test_shared(None, Arc::new(RootCertStore::empty())),
			orig80,
		)
		.await;
		let mut s = TcpStream::connect(p).await.expect("connect");
		let mut out = Vec::new();
		let _ = s.read_to_end(&mut out).await;
		assert_eq!(out, [] as [u8; 0]);
	}

	#[tokio::test]
	async fn domain_flow_via_socks_uses_remote_dns() {
		let origin = http_origin().await;
		let socks_listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let socks_addr = socks_listener.local_addr().expect("addr");
		let server = tokio::spawn(socks::tests::serve_one(socks_listener, Some(("u", "p")), origin));
		let policy = NetworkPolicy {
			allow_out: vec!["*.example.com".into()],
			deny_out: vec!["0.0.0.0/0".into()],
			egress_proxy: Some(EgressProxy {
				address:  socks_addr.to_string(),
				username: Some("u".into()),
				password: Some("p".into()),
				tls:      None,
			}),
			..NetworkPolicy::default()
		};
		// Original destination is a private address: irrelevant for a domain
		// match.
		let orig: SocketAddr = "10.1.2.3:80".parse().expect("addr");
		let p = proxy_once(
			compile(&policy, &[]),
			test_shared(None, Arc::new(RootCertStore::empty())),
			orig,
		)
		.await;
		let out = roundtrip(p, b"GET /x HTTP/1.1\r\nHost: api.example.com\r\n\r\n").await;
		assert!(out.contains("GET /x"), "{out}");
		let seen = server.await.expect("join").expect("socks");
		assert_eq!((seen.atyp, seen.host.as_str(), seen.port), (0x03, "api.example.com", 80));
	}

	#[tokio::test]
	async fn rules_intercept_tls_and_inject_headers() {
		let host = "api.example.com";
		let (origin, origin_roots) = mitm::tests::echo_origin(host).await;
		let dir = tempfile::tempdir().expect("tmp");
		let ca = Arc::new(Ca::load_or_create(dir.path()).expect("ca"));
		let mut rules = BTreeMap::new();
		rules.insert(host.to_string(), vec![NetworkRule {
			headers: BTreeMap::from([("x-token".into(), "secret".into())]),
		}]);
		let policy = NetworkPolicy { rules, ..NetworkPolicy::default() };
		// Dial the (loopback) origin through the CIDR path at port 443.
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let proxy_addr = listener.local_addr().expect("addr");
		let shared = test_shared(Some(ca.clone()), origin_roots);
		let compiled = compile(&policy, &["127.0.0.0/8"]);
		tokio::spawn(async move {
			let (guest, _) = listener.accept().await.expect("accept");
			// Forced original destination keeps :443 semantics while the
			// origin listens on an ephemeral port: pre-connect and splice.
			let mut prefix = Vec::new();
			let mut guest = guest;
			let host = sniff_host(&mut guest, &mut prefix, 443, Duration::from_secs(5))
				.await
				.expect("sniff")
				.expect("sni");
			let headers = compiled.rules_for(&host).expect("rule");
			let upstream = TcpStream::connect(origin).await.expect("origin");
			mitm::intercept(
				Box::new(Prefixed::new(prefix, guest)),
				Box::new(upstream),
				&host,
				mitm::header_overrides(headers).expect("headers"),
				ca.server_config(&host).expect("leaf"),
				shared.origin_tls.clone(),
			)
			.await
			.expect("intercept");
		});
		let body = mitm::tests::https_get(
			TcpStream::connect(proxy_addr).await.expect("connect"),
			host,
			&dir_ca_pem(dir.path()),
			"",
		)
		.await;
		assert!(body.contains("x-token: secret"), "{body}");
	}

	fn dir_ca_pem(dir: &std::path::Path) -> String {
		std::fs::read_to_string(dir.join("ca.pem")).expect("ca pem")
	}
}
