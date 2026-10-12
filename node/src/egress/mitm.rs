//! HTTPS header-transform interception for E2B `network.rules`.
//!
//! The node holds a CA (created once with rcgen, persisted under
//! `<data_dir>/egress/`, PEM handed to envd `/init` as `caBundle`) and mints
//! per-SNI leaf certificates on demand. An intercepted flow terminates TLS
//! toward the guest (rustls server, ALPN `http/1.1` only), opens verified TLS
//! to the real origin, and proxies HTTP/1.1 with hyper, overriding the
//! rule's headers on every request. `Upgrade` (e.g. WebSocket) requests are
//! spliced through after the 101.

use std::{
	collections::HashMap,
	fs, io,
	path::Path,
	sync::Arc,
	time::{Duration, Instant},
};

use anyhow::Context as _;
use bytes::Bytes;
use http::{HeaderName, HeaderValue, Request, Response, StatusCode};
use http_body_util::{BodyExt, Empty, combinators::BoxBody};
use hyper::body::Incoming;
use hyper_util::rt::TokioIo;
use parking_lot::Mutex;
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair, KeyUsagePurpose};
use rustls::{
	ClientConfig, RootCertStore, ServerConfig,
	pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer, ServerName, pem::PemObject},
};
use tokio_rustls::{TlsAcceptor, TlsConnector};

use super::BoxedIo;

const CA_KEY_FILE: &str = "ca-key.pem";
const CA_CERT_FILE: &str = "ca.pem";
const CA_COMMON_NAME: &str = "vmon egress CA";
const LEAF_VALIDITY: time::Duration = time::Duration::days(30);
/// Leaf configs are re-minted well before their certificate expires.
const LEAF_REUSE: Duration = Duration::from_hours(7 * 24);
const MAX_CACHED_LEAVES: usize = 1024;
const ALPN_HTTP11: &[u8] = b"http/1.1";

/// The node's interception CA plus a cache of per-hostname server configs.
pub struct Ca {
	issuer:   Issuer<'static, KeyPair>,
	cert_der: CertificateDer<'static>,
	pem:      String,
	leaf_key: KeyPair,
	leaves:   Mutex<HashMap<String, (Arc<ServerConfig>, Instant)>>,
}

fn ca_params() -> CertificateParams {
	let mut params = CertificateParams::default();
	params.distinguished_name = rcgen::DistinguishedName::new();
	params
		.distinguished_name
		.push(DnType::CommonName, CA_COMMON_NAME);
	params
		.distinguished_name
		.push(DnType::OrganizationName, "vmon");
	params.is_ca = IsCa::Ca(BasicConstraints::Constrained(0));
	params.key_usages = vec![
		KeyUsagePurpose::KeyCertSign,
		KeyUsagePurpose::CrlSign,
		KeyUsagePurpose::DigitalSignature,
	];
	params.not_before = rcgen::date_time_ymd(2024, 1, 1);
	params.not_after = rcgen::date_time_ymd(2049, 12, 31);
	params
}

fn provider() -> Arc<rustls::crypto::CryptoProvider> {
	Arc::new(rustls::crypto::ring::default_provider())
}

impl Ca {
	/// Load `<dir>/ca-key.pem` + `<dir>/ca.pem`, creating both (key 0600)
	/// on first use.
	pub fn load_or_create(dir: &Path) -> anyhow::Result<Self> {
		let key_path = dir.join(CA_KEY_FILE);
		let cert_path = dir.join(CA_CERT_FILE);
		let (key, pem) = if key_path.exists() && cert_path.exists() {
			let key = KeyPair::from_pem(
				&fs::read_to_string(&key_path)
					.with_context(|| format!("reading {}", key_path.display()))?,
			)
			.context("parsing egress CA key")?;
			(
				key,
				fs::read_to_string(&cert_path)
					.with_context(|| format!("reading {}", cert_path.display()))?,
			)
		} else {
			fs::create_dir_all(dir).with_context(|| format!("creating {}", dir.display()))?;
			let key = KeyPair::generate().context("generating egress CA key")?;
			let pem = ca_params()
				.self_signed(&key)
				.context("self-signing egress CA")?
				.pem();
			write_private(&key_path, key.serialize_pem().as_bytes())?;
			write_atomic(&cert_path, pem.as_bytes())?;
			(key, pem)
		};
		let cert_der =
			CertificateDer::from_pem_slice(pem.as_bytes()).context("parsing egress CA certificate")?;
		Ok(Self {
			issuer: Issuer::new(ca_params(), key),
			cert_der,
			pem,
			leaf_key: KeyPair::generate().context("generating egress leaf key")?,
			leaves: Mutex::new(HashMap::new()),
		})
	}

	/// PEM of the CA certificate (envd `/init` `caBundle`).
	pub fn pem(&self) -> &str {
		&self.pem
	}

	/// Server config presenting a leaf for `host` signed by this CA.
	pub fn server_config(&self, host: &str) -> anyhow::Result<Arc<ServerConfig>> {
		let now = Instant::now();
		let mut leaves = self.leaves.lock();
		if let Some((config, minted)) = leaves.get(host)
			&& now.duration_since(*minted) < LEAF_REUSE
		{
			return Ok(config.clone());
		}
		let mut params =
			CertificateParams::new(vec![host.to_string()]).context("leaf certificate names")?;
		params.distinguished_name = rcgen::DistinguishedName::new();
		params.distinguished_name.push(DnType::CommonName, host);
		params.use_authority_key_identifier_extension = true;
		params.extended_key_usages = vec![rcgen::ExtendedKeyUsagePurpose::ServerAuth];
		params.key_usages = vec![KeyUsagePurpose::DigitalSignature];
		let now_utc = time::OffsetDateTime::now_utc();
		params.not_before = now_utc - time::Duration::days(1);
		params.not_after = now_utc + LEAF_VALIDITY;
		let leaf = params
			.signed_by(&self.leaf_key, &self.issuer)
			.context("signing leaf certificate")?;
		let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(self.leaf_key.serialize_der()));
		let mut config = ServerConfig::builder_with_provider(provider())
			.with_safe_default_protocol_versions()?
			.with_no_client_auth()
			.with_single_cert(vec![leaf.der().clone(), self.cert_der.clone()], key)?;
		config.alpn_protocols = vec![ALPN_HTTP11.to_vec()];
		let config = Arc::new(config);
		if leaves.len() >= MAX_CACHED_LEAVES {
			leaves.retain(|_, (_, minted)| now.duration_since(*minted) < LEAF_REUSE);
			if leaves.len() >= MAX_CACHED_LEAVES {
				leaves.clear();
			}
		}
		leaves.insert(host.to_string(), (config.clone(), now));
		Ok(config)
	}
}

fn write_atomic(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
	let tmp = path.with_extension("tmp");
	fs::write(&tmp, bytes).with_context(|| format!("writing {}", tmp.display()))?;
	fs::rename(&tmp, path).with_context(|| format!("renaming {}", tmp.display()))
}

fn write_private(path: &Path, bytes: &[u8]) -> anyhow::Result<()> {
	use std::io::Write as _;
	#[cfg(unix)]
	use std::os::unix::fs::OpenOptionsExt as _;
	let tmp = path.with_extension("tmp");
	let mut options = fs::OpenOptions::new();
	options.write(true).create(true).truncate(true);
	#[cfg(unix)]
	options.mode(0o600);
	let mut file = options
		.open(&tmp)
		.with_context(|| format!("creating {}", tmp.display()))?;
	file.write_all(bytes)?;
	file.sync_all()?;
	fs::rename(&tmp, path).with_context(|| format!("renaming {}", tmp.display()))
}

/// Client config used toward real origins (HTTP/1.1 only).
pub fn upstream_client_config(roots: Arc<RootCertStore>) -> anyhow::Result<Arc<ClientConfig>> {
	let mut config = ClientConfig::builder_with_provider(provider())
		.with_safe_default_protocol_versions()?
		.with_root_certificates(roots)
		.with_no_client_auth();
	config.alpn_protocols = vec![ALPN_HTTP11.to_vec()];
	Ok(Arc::new(config))
}

/// Mozilla roots (webpki-roots) used to verify origins and TLS proxies.
pub fn system_roots() -> Arc<RootCertStore> {
	Arc::new(webpki_roots::TLS_SERVER_ROOTS.iter().cloned().collect())
}

/// Pre-validated header overrides for one intercepted flow.
pub type HeaderOverrides = Arc<[(HeaderName, HeaderValue)]>;

/// Convert compiled rule headers (already checked by `policy::validate`).
pub fn header_overrides(headers: &[(String, String)]) -> io::Result<HeaderOverrides> {
	headers
		.iter()
		.map(|(name, value)| {
			let name = HeaderName::from_bytes(name.as_bytes())
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
			let value = HeaderValue::from_str(value)
				.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
			Ok((name, value))
		})
		.collect()
}

type ProxyBody = BoxBody<Bytes, hyper::Error>;

fn bad_gateway() -> Response<ProxyBody> {
	let mut response = Response::new(
		Empty::<Bytes>::new()
			.map_err(|never| match never {})
			.boxed(),
	);
	*response.status_mut() = StatusCode::BAD_GATEWAY;
	response
}

/// Terminate the guest's TLS (`client`, positioned at its `ClientHello`) as
/// `host`, open TLS to the origin over `upstream`, and proxy HTTP/1.1 with
/// `overrides` applied to every request.
pub async fn intercept(
	client: BoxedIo,
	upstream: BoxedIo,
	host: &str,
	overrides: HeaderOverrides,
	server: Arc<ServerConfig>,
	origin: Arc<ClientConfig>,
) -> io::Result<()> {
	let server_name = ServerName::try_from(host.to_string())
		.map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
	let (guest, origin) = tokio::try_join!(
		TlsAcceptor::from(server).accept(client),
		TlsConnector::from(origin).connect(server_name, upstream),
	)?;
	let (sender, connection) =
		hyper::client::conn::http1::handshake::<_, Incoming>(TokioIo::new(origin))
			.await
			.map_err(io::Error::other)?;
	let upstream_task = tokio::spawn(connection.with_upgrades());
	let sender = Arc::new(tokio::sync::Mutex::new(sender));
	let service = hyper::service::service_fn(move |mut request: Request<Incoming>| {
		let sender = sender.clone();
		let overrides = overrides.clone();
		async move {
			for (name, value) in overrides.iter() {
				request.headers_mut().insert(name.clone(), value.clone());
			}
			let guest_upgrade = request
				.headers()
				.contains_key(http::header::UPGRADE)
				.then(|| hyper::upgrade::on(&mut request));
			let mut sender = sender.lock().await;
			let response = match sender.ready().await {
				Ok(()) => sender.send_request(request).await,
				Err(e) => Err(e),
			};
			drop(sender);
			let mut response = match response {
				Ok(response) => response,
				Err(e) => {
					tracing::debug!("egress MITM upstream request failed: {e}");
					return Ok::<_, std::convert::Infallible>(bad_gateway());
				},
			};
			if response.status() == StatusCode::SWITCHING_PROTOCOLS
				&& let Some(guest_upgrade) = guest_upgrade
			{
				let origin_upgrade = hyper::upgrade::on(&mut response);
				tokio::spawn(async move {
					if let (Ok(guest), Ok(origin)) = tokio::join!(guest_upgrade, origin_upgrade) {
						let _ = tokio::io::copy_bidirectional(
							&mut TokioIo::new(guest),
							&mut TokioIo::new(origin),
						)
						.await;
					}
				});
			}
			Ok(response.map(BodyExt::boxed))
		}
	});
	let served = hyper::server::conn::http1::Builder::new()
		.serve_connection(TokioIo::new(guest), service)
		.with_upgrades()
		.await;
	upstream_task.abort();
	served.map_err(io::Error::other)
}

#[cfg(test)]
pub(crate) mod tests {
	use std::net::SocketAddr;

	use tokio::{
		io::{AsyncReadExt, AsyncWriteExt},
		net::{TcpListener, TcpStream},
	};

	use super::*;

	/// An HTTPS origin for `host` that answers every request with the
	/// received request head (so tests can assert on injected headers).
	/// Returns its address and the root that verifies it.
	pub(in crate::egress) async fn echo_origin(host: &str) -> (SocketAddr, Arc<RootCertStore>) {
		let ca_key = KeyPair::generate().expect("key");
		let ca = ca_params().self_signed(&ca_key).expect("ca");
		let issuer = Issuer::new(ca_params(), ca_key);
		let leaf_key = KeyPair::generate().expect("key");
		let leaf = CertificateParams::new(vec![host.to_string()])
			.expect("params")
			.signed_by(&leaf_key, &issuer)
			.expect("leaf");
		let mut roots = RootCertStore::empty();
		roots.add(ca.der().clone()).expect("root");
		let mut config = ServerConfig::builder_with_provider(provider())
			.with_safe_default_protocol_versions()
			.expect("versions")
			.with_no_client_auth()
			.with_single_cert(
				vec![leaf.der().clone()],
				PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(leaf_key.serialize_der())),
			)
			.expect("cert");
		config.alpn_protocols = vec![ALPN_HTTP11.to_vec()];
		let acceptor = TlsAcceptor::from(Arc::new(config));
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let addr = listener.local_addr().expect("addr");
		tokio::spawn(async move {
			while let Ok((tcp, _)) = listener.accept().await {
				let acceptor = acceptor.clone();
				tokio::spawn(async move {
					let Ok(tls) = acceptor.accept(tcp).await else {
						return;
					};
					let service = hyper::service::service_fn(|request: Request<Incoming>| async move {
						use std::fmt::Write as _;
						let mut head = format!("{} {}\n", request.method(), request.uri());
						for (name, value) in request.headers() {
							let _ = writeln!(head, "{name}: {}", value.to_str().unwrap_or("?"));
						}
						Ok::<_, std::convert::Infallible>(Response::new(http_body_util::Full::new(
							Bytes::from(head),
						)))
					});
					let _ = hyper::server::conn::http1::Builder::new()
						.serve_connection(TokioIo::new(tls), service)
						.await;
				});
			}
		});
		(addr, Arc::new(roots))
	}

	/// Send one HTTP/1.1 request over TLS trusting `root` and return the body.
	pub(in crate::egress) async fn https_get(
		tcp: TcpStream,
		host: &str,
		root: &str,
		extra: &str,
	) -> String {
		let mut roots = RootCertStore::empty();
		for cert in crate::egress::policy::parse_pem_certs(root).expect("pem") {
			roots.add(cert).expect("root");
		}
		let config = ClientConfig::builder_with_provider(provider())
			.with_safe_default_protocol_versions()
			.expect("versions")
			.with_root_certificates(roots)
			.with_no_client_auth();
		let mut tls = TlsConnector::from(Arc::new(config))
			.connect(ServerName::try_from(host.to_string()).expect("name"), tcp)
			.await
			.expect("guest TLS handshake must trust the egress CA");
		let request = format!(
			"GET /p HTTP/1.1\r\nHost: {host}\r\nAuthorization: guest\r\n{extra}Connection: \
			 close\r\n\r\n"
		);
		tls.write_all(request.as_bytes()).await.expect("write");
		let mut response = Vec::new();
		let _ = tls.read_to_end(&mut response).await;
		String::from_utf8_lossy(&response).into_owned()
	}

	#[test]
	fn ca_persists_and_reloads() {
		let dir = tempfile::tempdir().expect("tmp");
		let first = Ca::load_or_create(dir.path()).expect("create");
		let second = Ca::load_or_create(dir.path()).expect("load");
		assert_eq!(first.pem(), second.pem());
		#[cfg(unix)]
		{
			use std::os::unix::fs::PermissionsExt as _;
			let mode = fs::metadata(dir.path().join(CA_KEY_FILE))
				.expect("meta")
				.permissions()
				.mode();
			assert_eq!(mode & 0o777, 0o600);
		}
		let a = second.server_config("a.example.com").expect("leaf");
		let b = second.server_config("a.example.com").expect("leaf");
		assert!(Arc::ptr_eq(&a, &b), "leaf configs are cached per host");
	}

	#[tokio::test]
	async fn injects_headers_end_to_end() {
		let host = "api.example.com";
		let (origin_addr, origin_roots) = echo_origin(host).await;
		let dir = tempfile::tempdir().expect("tmp");
		let ca = Arc::new(Ca::load_or_create(dir.path()).expect("ca"));
		let overrides = header_overrides(&[
			("authorization".into(), "Bearer injected".into()),
			("x-extra".into(), "1".into()),
		])
		.expect("headers");
		let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
		let proxy_addr = listener.local_addr().expect("addr");
		let ca2 = ca.clone();
		tokio::spawn(async move {
			let (guest, _) = listener.accept().await.expect("accept");
			let upstream = TcpStream::connect(origin_addr).await.expect("origin");
			let server = ca2.server_config(host).expect("leaf");
			let origin = upstream_client_config(origin_roots).expect("client");
			intercept(Box::new(guest), Box::new(upstream), host, overrides, server, origin)
				.await
				.expect("intercept");
		});
		let body = https_get(
			TcpStream::connect(proxy_addr).await.expect("connect"),
			host,
			ca.pem(),
			"X-Keep: yes\r\n",
		)
		.await;
		assert!(body.starts_with("HTTP/1.1 200"), "{body}");
		assert!(body.contains("authorization: Bearer injected"), "{body}");
		assert!(!body.contains("authorization: guest"), "{body}");
		assert!(body.contains("x-extra: 1"), "{body}");
		assert!(body.contains("x-keep: yes"), "{body}");
	}
}
