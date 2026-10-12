//! Reverse proxy from a public request to a guest port through a node tunnel
//! (ARCHITECTURE.md §9.5): HTTP/1.1 over `vmon-tunnel`, streaming bodies,
//! Upgrade/WebSocket splice, optional TLS to the guest.

use std::{
	pin::Pin,
	sync::{Arc, LazyLock},
	task::{Context, Poll},
	time::Duration,
};

use axum::{
	body::Body,
	extract::Request,
	http::{HeaderMap, HeaderName, HeaderValue, StatusCode, Uri, Version, header},
	response::Response,
};
use hyper::upgrade::OnUpgrade;
use hyper_util::rt::TokioIo;
use rustls::{
	ClientConfig, DigitallySignedStruct, SignatureScheme,
	client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
	pki_types::{CertificateDer, ServerName, UnixTime},
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use vmon_node::client::{NodeClient, NodeError};

use super::{response::ProxyError, target::Target};

/// Tunnel dial attempts while the guest port is not yet listening.
const DIAL_ATTEMPTS: u32 = 5;
/// Linear backoff step between dial attempts (100, 200, 300, 400 ms).
const DIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Upstream byte stream (plain tunnel or TLS over it).
trait Stream: AsyncRead + AsyncWrite + Send + Unpin {}
impl<T: AsyncRead + AsyncWrite + Send + Unpin> Stream for T {}

/// TLS client for `httpsPorts`; guest certificates are not verified.
static GUEST_TLS: LazyLock<Arc<ClientConfig>> = LazyLock::new(|| {
	let provider = Arc::new(rustls::crypto::ring::default_provider());
	let mut config = ClientConfig::builder_with_provider(provider.clone())
		.with_safe_default_protocol_versions()
		.expect("ring supports the default protocol versions")
		.dangerous()
		.with_custom_certificate_verifier(Arc::new(NoVerify(provider)))
		.with_no_client_auth();
	config.alpn_protocols = vec![b"http/1.1".to_vec()];
	Arc::new(config)
});

/// How the request reaches the guest.
pub struct Upstream<'a> {
	pub node:        &'a NodeClient,
	pub target:      &'a Target,
	/// Speak TLS to the guest port (`httpsPorts`), certificate unchecked.
	pub https:       bool,
	/// Upstream `Host` override (`maskRequestHost`); the original goes to
	/// `X-Forwarded-Host`.
	pub masked_host: Option<String>,
}

/// Forwards one request; every failure is rendered as a proxy error body.
/// Dropping the returned body (client disconnect) closes the guest connection.
pub async fn forward(upstream: Upstream<'_>, mut req: Request) -> Response {
	let target = upstream.target;
	let client_upgrade = wants_upgrade(req.headers()).then(|| hyper::upgrade::on(&mut req));
	let request_headers = req.headers().clone();
	let (req, sni) =
		match rewrite_request(req, upstream.masked_host.as_deref(), client_upgrade.is_some()) {
			Ok(pair) => pair,
			Err(message) => return super::response::bad_request(message),
		};
	let port_closed =
		|| ProxyError::port_closed(&target.sandbox_id, target.port).render(&request_headers);

	let stream = match dial(upstream.node, target).await {
		Ok(stream) => stream,
		Err(error) => return error.render(&request_headers),
	};
	let stream: Box<dyn Stream> = if upstream.https {
		let name = ServerName::try_from(sni)
			.unwrap_or_else(|_| ServerName::try_from("localhost").expect("valid name"));
		match tokio_rustls::TlsConnector::from(GUEST_TLS.clone())
			.connect(name, stream)
			.await
		{
			Ok(tls) => Box::new(CloseNotifyOptional(tls)),
			Err(error) => {
				tracing::debug!(%error, sandbox = %target.sandbox_id, port = target.port, "guest TLS handshake failed");
				return port_closed();
			},
		}
	} else {
		stream
	};

	let Ok((mut sender, connection)) = guest_http1().handshake(TokioIo::new(stream)).await else {
		return port_closed();
	};
	tokio::spawn(async move {
		if let Err(error) = connection.with_upgrades().await {
			tracing::debug!(%error, "guest connection ended");
		}
	});
	let mut response = match sender.send_request(req).await {
		Ok(response) => response,
		Err(error) => {
			tracing::debug!(%error, sandbox = %target.sandbox_id, port = target.port, "upstream request failed");
			return port_closed();
		},
	};
	drop(sender);

	if response.status() == StatusCode::SWITCHING_PROTOCOLS {
		let Some(client_upgrade) = client_upgrade else {
			return port_closed();
		};
		let upstream_upgrade = hyper::upgrade::on(&mut response);
		tokio::spawn(splice(client_upgrade, upstream_upgrade));
		let (parts, _) = response.into_parts();
		return Response::from_parts(parts, Body::empty());
	}
	let (mut parts, body) = response.into_parts();
	strip_hop_by_hop(&mut parts.headers);
	Response::from_parts(parts, Body::new(body))
}

/// Bidirectional byte copy between the client and guest after a 101.
async fn splice(client: OnUpgrade, upstream: OnUpgrade) {
	let (client, upstream) = match tokio::try_join!(client, upstream) {
		Ok(pair) => pair,
		Err(error) => {
			tracing::debug!(%error, "upgrade handoff failed");
			return;
		},
	};
	let (mut client, mut upstream) = (TokioIo::new(client), TokioIo::new(upstream));
	if let Err(error) = tokio::io::copy_bidirectional(&mut client, &mut upstream).await {
		tracing::debug!(%error, "upgraded stream ended");
	}
}

/// Opens the node tunnel, retrying while the guest port refuses connections.
async fn dial(node: &NodeClient, target: &Target) -> Result<Box<dyn Stream>, ProxyError> {
	let mut attempt = 1;
	loop {
		match node.open_tunnel(&target.sandbox_id, target.port).await {
			Ok(io) => return Ok(Box::new(io)),
			// 404 unknown, 409 paused on the node (no VM): the sandbox is not running.
			Err(error @ NodeError::Api { status: 404 | 409, .. }) => {
				tracing::debug!(%error, sandbox = %target.sandbox_id, "tunnel target not running");
				return Err(ProxyError::not_found(&target.sandbox_id));
			},
			Err(NodeError::Api { code, .. }) if code == "port_closed" && attempt < DIAL_ATTEMPTS => {
				tokio::time::sleep(DIAL_BACKOFF * attempt).await;
				attempt += 1;
			},
			Err(error) => {
				tracing::debug!(%error, sandbox = %target.sandbox_id, port = target.port, "tunnel dial failed");
				return Err(ProxyError::port_closed(&target.sandbox_id, target.port));
			},
		}
	}
}

/// Converts the public request into an origin-form HTTP/1.1 request for the
/// guest. Returns it with the host name used for TLS SNI.
fn rewrite_request(
	req: Request,
	masked_host: Option<&str>,
	upgrade: bool,
) -> Result<(Request, String), &'static str> {
	let (mut parts, body) = req.into_parts();
	let authority =
		super::target::request_authority(&parts.uri, &parts.headers).ok_or("Invalid host")?;
	let upgrade_value = parts.headers.get(header::UPGRADE).cloned();
	strip_hop_by_hop(&mut parts.headers);
	if upgrade && let Some(value) = upgrade_value {
		parts
			.headers
			.insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
		parts.headers.insert(header::UPGRADE, value);
	}
	let host = match masked_host {
		Some(mask) => {
			let forwarded = HeaderValue::from_str(&authority).map_err(|_| "Invalid host")?;
			parts
				.headers
				.insert(HeaderName::from_static("x-forwarded-host"), forwarded);
			mask.to_owned()
		},
		None => authority,
	};
	parts
		.headers
		.insert(header::HOST, HeaderValue::from_str(&host).map_err(|_| "Invalid host")?);
	let path = parts.uri.path_and_query().map_or("/", |p| p.as_str());
	parts.uri = Uri::try_from(path).map_err(|_| "Invalid path")?;
	parts.version = Version::HTTP_11;
	let sni = super::target::strip_port(&host).to_owned();
	Ok((Request::from_parts(parts, body), sni))
}

fn wants_upgrade(headers: &HeaderMap) -> bool {
	headers.contains_key(header::UPGRADE)
		&& connection_tokens(headers).any(|t| t.eq_ignore_ascii_case("upgrade"))
}

fn connection_tokens(headers: &HeaderMap) -> impl Iterator<Item = &str> {
	headers
		.get_all(header::CONNECTION)
		.iter()
		.filter_map(|v| v.to_str().ok())
		.flat_map(|v| v.split(','))
		.map(str::trim)
		.filter(|t| !t.is_empty())
}

/// Removes RFC 9110 §7.6.1 connection-specific headers, including any
/// listed in `Connection`.
fn strip_hop_by_hop(headers: &mut HeaderMap) {
	let named: Vec<HeaderName> = connection_tokens(headers)
		.filter_map(|t| HeaderName::try_from(t).ok())
		.collect();
	for name in named {
		headers.remove(name);
	}
	for name in [
		header::CONNECTION,
		header::PROXY_AUTHENTICATE,
		header::PROXY_AUTHORIZATION,
		header::TE,
		header::TRAILER,
		header::TRANSFER_ENCODING,
		header::UPGRADE,
	] {
		headers.remove(name);
	}
	headers.remove("keep-alive");
	headers.remove("proxy-connection");
}

/// Guest TLS stream that reads a TCP close without `close_notify` as a clean
/// EOF, as Go's `crypto/tls` (upstream proxy) and browsers do.
///
/// Common guest servers (Python `http.server` + `ssl.wrap_socket`) close that
/// way after a close-delimited HTTP/1.0 response. rustls reports it as
/// `UnexpectedEof`, which would turn a complete response into a body error
/// (an `RST_STREAM` for HTTP/2 clients). Truncation of length-delimited or
/// chunked bodies is still detected by the HTTP layer.
struct CloseNotifyOptional<S>(S);

impl<S: AsyncRead + Unpin> AsyncRead for CloseNotifyOptional<S> {
	fn poll_read(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		buf: &mut ReadBuf<'_>,
	) -> Poll<std::io::Result<()>> {
		match Pin::new(&mut self.0).poll_read(cx, buf) {
			Poll::Ready(Err(error)) if error.kind() == std::io::ErrorKind::UnexpectedEof => {
				Poll::Ready(Ok(()))
			},
			other => other,
		}
	}
}

impl<S: AsyncWrite + Unpin> AsyncWrite for CloseNotifyOptional<S> {
	fn poll_write(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		data: &[u8],
	) -> Poll<std::io::Result<usize>> {
		Pin::new(&mut self.0).poll_write(cx, data)
	}

	fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.0).poll_flush(cx)
	}

	fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
		Pin::new(&mut self.0).poll_shutdown(cx)
	}

	fn poll_write_vectored(
		mut self: Pin<&mut Self>,
		cx: &mut Context<'_>,
		bufs: &[std::io::IoSlice<'_>],
	) -> Poll<std::io::Result<usize>> {
		Pin::new(&mut self.0).poll_write_vectored(cx, bufs)
	}

	fn is_write_vectored(&self) -> bool {
		self.0.is_write_vectored()
	}
}

/// Accepts any guest certificate (upstream `InsecureSkipVerify` for
/// `httpsPorts`); handshake signatures are still checked.
#[derive(Debug)]
struct NoVerify(Arc<rustls::crypto::CryptoProvider>);

impl ServerCertVerifier for NoVerify {
	fn verify_server_cert(
		&self,
		_end_entity: &CertificateDer<'_>,
		_intermediates: &[CertificateDer<'_>],
		_server_name: &ServerName<'_>,
		_ocsp_response: &[u8],
		_now: UnixTime,
	) -> Result<ServerCertVerified, rustls::Error> {
		Ok(ServerCertVerified::assertion())
	}

	fn verify_tls12_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, rustls::Error> {
		rustls::crypto::verify_tls12_signature(
			message,
			cert,
			dss,
			&self.0.signature_verification_algorithms,
		)
	}

	fn verify_tls13_signature(
		&self,
		message: &[u8],
		cert: &CertificateDer<'_>,
		dss: &DigitallySignedStruct,
	) -> Result<HandshakeSignatureValid, rustls::Error> {
		rustls::crypto::verify_tls13_signature(
			message,
			cert,
			dss,
			&self.0.signature_verification_algorithms,
		)
	}

	fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
		self.0.signature_verification_algorithms.supported_schemes()
	}
}

/// HTTP/1 client for guest connections. Header names go out title-cased
/// (`Host`, `User-Agent`) like upstream's Go proxy, so guest servers that echo
/// raw header names see what they would see on E2B.
fn guest_http1() -> hyper::client::conn::http1::Builder {
	let mut builder = hyper::client::conn::http1::Builder::new();
	builder.title_case_headers(true);
	builder
}

#[cfg(test)]
mod tests {
	use super::*;

	fn request(uri: &str, headers: &[(&str, &str)]) -> Request {
		let mut builder = Request::builder().uri(uri);
		for (k, v) in headers {
			builder = builder.header(*k, *v);
		}
		builder.body(Body::empty()).unwrap()
	}

	#[test]
	fn rewrite_preserves_host_and_strips_hop_by_hop() {
		let req = request("http://8080-iabc.e2b.local/a?b=1", &[
			("host", "8080-iabc.e2b.local"),
			("connection", "keep-alive, x-drop"),
			("x-drop", "1"),
			("x-keep", "2"),
			("te", "trailers"),
		]);
		let (out, sni) = rewrite_request(req, None, false).unwrap();
		assert_eq!(out.uri(), "/a?b=1");
		assert_eq!(out.version(), Version::HTTP_11);
		assert_eq!(out.headers()[header::HOST], "8080-iabc.e2b.local");
		assert!(!out.headers().contains_key("x-drop") && !out.headers().contains_key("connection"));
		assert!(!out.headers().contains_key("te") && !out.headers().contains_key("x-forwarded-host"));
		assert_eq!(out.headers()["x-keep"], "2");
		assert_eq!(sni, "8080-iabc.e2b.local");
	}

	#[test]
	fn rewrite_masks_host_and_keeps_upgrade() {
		let req = request("/ws", &[
			("host", "3000-iabc.e2b.local:443"),
			("connection", "Upgrade"),
			("upgrade", "websocket"),
		]);
		let (out, sni) = rewrite_request(req, Some("localhost:3000"), true).unwrap();
		assert_eq!(out.headers()[header::HOST], "localhost:3000");
		assert_eq!(out.headers()["x-forwarded-host"], "3000-iabc.e2b.local:443");
		assert_eq!(out.headers()[header::CONNECTION], "upgrade");
		assert_eq!(out.headers()[header::UPGRADE], "websocket");
		assert_eq!(sni, "localhost");
	}

	#[tokio::test]
	async fn guest_requests_carry_title_cased_header_names() {
		use tokio::io::{AsyncReadExt, AsyncWriteExt};

		let (client, mut guest) = tokio::io::duplex(4096);
		let (mut sender, connection) = guest_http1()
			.handshake::<_, Body>(TokioIo::new(client))
			.await
			.unwrap();
		tokio::spawn(connection);
		let req =
			request("/", &[("host", "custom-host.example.com:8080"), ("x-forwarded-host", "h")]);
		let sent = tokio::spawn(async move { sender.send_request(req).await });
		let mut head = Vec::new();
		while !head.ends_with(b"\r\n\r\n") {
			let mut byte = [0];
			guest.read_exact(&mut byte).await.unwrap();
			head.push(byte[0]);
		}
		let head = String::from_utf8(head).unwrap();
		assert!(head.contains("\r\nHost: custom-host.example.com:8080\r\n"), "{head}");
		assert!(head.contains("\r\nX-Forwarded-Host: h\r\n"), "{head}");
		guest
			.write_all(b"HTTP/1.1 204 No Content\r\n\r\n")
			.await
			.unwrap();
		assert_eq!(sent.await.unwrap().unwrap().status(), StatusCode::NO_CONTENT);
	}

	#[test]
	fn upgrade_headers_dropped_when_not_upgrading() {
		let req = request("/", &[("host", "h.x"), ("upgrade", "h2c")]);
		let (out, _) = rewrite_request(req, None, false).unwrap();
		assert!(!out.headers().contains_key(header::UPGRADE));
	}

	#[test]
	fn http2_authority_becomes_host() {
		let req = request("https://49983-iabc.e2b.local/health", &[]);
		let (out, _) = rewrite_request(req, None, false).unwrap();
		assert_eq!(out.headers()[header::HOST], "49983-iabc.e2b.local");
		assert_eq!(out.uri(), "/health");
	}

	#[test]
	fn upgrade_detection_needs_connection_token() {
		let h = |pairs: &[(&'static str, &'static str)]| {
			let mut m = HeaderMap::new();
			for (k, v) in pairs {
				m.append(*k, HeaderValue::from_static(v));
			}
			m
		};
		assert!(wants_upgrade(&h(&[
			("connection", "keep-alive, Upgrade"),
			("upgrade", "websocket")
		])));
		assert!(!wants_upgrade(&h(&[("upgrade", "websocket")])));
		assert!(!wants_upgrade(&h(&[("connection", "upgrade")])));
	}
}
