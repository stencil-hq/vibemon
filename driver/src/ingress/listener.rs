//! Public listeners (ARCHITECTURE.md §3): HTTP/1.1 + h2c on the plain port,
//! TLS with ALPN `h2`/`http/1.1` on the TLS port. Started by `vmon serve`.

use std::{future::Future, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use anyhow::Context as _;
use axum::Router;
use hyper_util::{
	rt::{TokioExecutor, TokioIo},
	server::{conn::auto, graceful::GracefulShutdown},
	service::TowerToHyperService,
};
use rustls::{
	ServerConfig,
	pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
};
use tokio::net::TcpListener;
use tokio_rustls::TlsAcceptor;

/// How long in-flight connections may drain after shutdown is requested.
const DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// PEM certificate chain and private key for the TLS listener.
#[derive(Clone, Debug)]
pub struct TlsFiles {
	pub cert: PathBuf,
	pub key:  PathBuf,
}

/// Serves `router` on `addr` until `shutdown` resolves, then drains.
///
/// Draining is bounded by [`DRAIN_TIMEOUT`]. `tls = None` serves plain
/// HTTP/1.1 and h2c (prior knowledge); otherwise TLS negotiates h2/http1.1.
pub async fn serve(
	addr: SocketAddr,
	tls: Option<TlsFiles>,
	router: Router,
	shutdown: impl Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
	let acceptor = tls.as_ref().map(load_tls).transpose()?;
	let listener = TcpListener::bind(addr)
		.await
		.with_context(|| format!("binding {addr}"))?;
	tracing::info!(%addr, tls = acceptor.is_some(), "public listener up");
	serve_listener(listener, acceptor, router, shutdown).await;
	Ok(())
}

/// Accept loop over an already-bound listener; used by [`serve`] and by
/// tests that bind port 0 and need the chosen address.
pub async fn serve_listener(
	listener: TcpListener,
	acceptor: Option<TlsAcceptor>,
	router: Router,
	shutdown: impl Future<Output = ()> + Send + 'static,
) {
	let graceful = GracefulShutdown::new();
	let builder = Arc::new(auto::Builder::new(TokioExecutor::new()));
	tokio::pin!(shutdown);
	loop {
		let (stream, peer) = tokio::select! {
			() = &mut shutdown => break,
			accepted = listener.accept() => match accepted {
				Ok(accepted) => accepted,
				Err(error) => {
					// Transient (EMFILE, ECONNABORTED): back off and keep serving.
					tracing::warn!(%error, "accept failed");
					tokio::time::sleep(Duration::from_millis(50)).await;
					continue;
				},
			},
		};
		stream.set_nodelay(true).ok();
		let service = TowerToHyperService::new(router.clone());
		let builder = builder.clone();
		let watcher = graceful.watcher();
		let acceptor = acceptor.clone();
		tokio::spawn(async move {
			let result = match acceptor {
				None => {
					watcher
						.watch(
							builder
								.serve_connection_with_upgrades(TokioIo::new(stream), service)
								.into_owned(),
						)
						.await
				},
				Some(acceptor) => match acceptor.accept(stream).await {
					Ok(tls) => {
						watcher
							.watch(
								builder
									.serve_connection_with_upgrades(TokioIo::new(tls), service)
									.into_owned(),
							)
							.await
					},
					Err(error) => {
						tracing::debug!(%error, %peer, "TLS handshake failed");
						return;
					},
				},
			};
			if let Err(error) = result {
				tracing::debug!(%error, %peer, "connection ended with error");
			}
		});
	}
	drop(listener);
	if tokio::time::timeout(DRAIN_TIMEOUT, graceful.shutdown())
		.await
		.is_err()
	{
		tracing::warn!("public connections did not drain in time");
	}
}

/// Builds the rustls acceptor from PEM files (ALPN h2 then http/1.1).
pub fn load_tls(files: &TlsFiles) -> anyhow::Result<TlsAcceptor> {
	let certs = CertificateDer::pem_file_iter(&files.cert)
		.with_context(|| format!("reading {}", files.cert.display()))?
		.collect::<Result<Vec<_>, _>>()
		.with_context(|| format!("parsing {}", files.cert.display()))?;
	anyhow::ensure!(!certs.is_empty(), "{} contains no certificate", files.cert.display());
	let key = PrivateKeyDer::from_pem_file(&files.key)
		.with_context(|| format!("reading {}", files.key.display()))?;
	let mut config =
		ServerConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
			.with_safe_default_protocol_versions()
			.context("TLS protocol versions")?
			.with_no_client_auth()
			.with_single_cert(certs, key)
			.context("TLS certificate/key")?;
	config.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
	Ok(TlsAcceptor::from(Arc::new(config)))
}
