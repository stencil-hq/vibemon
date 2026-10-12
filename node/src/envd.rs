//! Node-side envd client (guest TCP 49983) over GC4 tunnels.
//!
//! `POST /init` after every create/resume (ARCHITECTURE.md §8.2), `/health`,
//! and the 5 s `/metrics` scrape (§8.7). Each call opens a fresh tunnel and
//! speaks HTTP/1.1 with hyper.

use std::time::Duration;

use anyhow::{Context, anyhow, bail};
use bytes::Bytes;
use http::{Method, Request, Response, StatusCode, header};
use http_body_util::{BodyExt, Full};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::json;
use tokio::time::Instant;

use crate::{api::GuestInit, gc4::ConnectError, vm::Vm};

/// envd's guest port.
pub const ENVD_PORT: u16 = 49983;
/// `/init` retry cadence, per-try connect budget, and total budget.
const INIT_RETRY: Duration = Duration::from_millis(5);
const INIT_TRY: Duration = Duration::from_millis(50);
const INIT_BUDGET: Duration = Duration::from_secs(10);
/// Timeout of plain requests (health, metrics).
const REQUEST_TIMEOUT: Duration = Duration::from_secs(10);

/// envd `/metrics` body (subset the node records).
#[derive(Debug, Deserialize)]
pub struct EnvdMetrics {
	#[serde(default)]
	pub cpu_count:    u32,
	#[serde(default)]
	pub cpu_used_pct: f64,
	#[serde(default)]
	pub mem_total:    u64,
	#[serde(default)]
	pub mem_used:     u64,
	#[serde(default)]
	pub mem_cache:    u64,
	#[serde(default)]
	pub disk_used:    u64,
	#[serde(default)]
	pub disk_total:   u64,
}

/// envd client bound to one VM and (optionally) its access token.
#[derive(Clone, Debug)]
pub struct EnvdClient {
	vm:    Vm,
	token: Option<String>,
}

/// Why one HTTP exchange failed.
enum TryError {
	/// Tunnel or connection setup failed (envd not listening yet): retryable.
	Connect(String),
	/// The request was delivered but failed.
	Request(anyhow::Error),
}

/// JSON body of `POST /init` with a fresh `timestamp`.
fn init_body(init: &GuestInit) -> Bytes {
	let mut body = json!({
		"envVars": init.env_vars,
		"accessToken": init.envd_access_token,
		"timestamp": chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true),
		"sandboxId": init.sandbox_id,
		"templateId": init.template_id,
	});
	if let Some(user) = &init.default_user {
		body["defaultUser"] = json!(user);
	}
	if let Some(workdir) = &init.default_workdir {
		body["defaultWorkdir"] = json!(workdir);
	}
	if let Some(ca) = &init.ca_bundle {
		body["caBundle"] = json!(ca);
	}
	Bytes::from(body.to_string())
}

impl EnvdClient {
	/// Client without an access token.
	pub const fn new(vm: Vm) -> Self {
		Self { vm, token: None }
	}

	/// Client sending `X-Access-Token: token` (empty = none).
	pub fn with_token(vm: Vm, token: &str) -> Self {
		Self { vm, token: (!token.is_empty()).then(|| token.to_owned()) }
	}

	async fn exchange(
		&self,
		request: Request<Full<Bytes>>,
		connect_timeout: Duration,
		timeout: Duration,
	) -> Result<Response<Bytes>, TryError> {
		let connect = async {
			let stream = self
				.vm
				.tcp_connect(ENVD_PORT)
				.await
				.map_err(|error| match error {
					ConnectError::Refused => "envd port is not open yet".to_owned(),
					ConnectError::Failed(message) => message,
				})?;
			hyper::client::conn::http1::handshake(TokioIo::new(stream))
				.await
				.map_err(|e| e.to_string())
		};
		let (mut sender, connection) = tokio::time::timeout(connect_timeout, connect)
			.await
			.map_err(|_| TryError::Connect(format!("no envd connection within {connect_timeout:?}")))?
			.map_err(TryError::Connect)?;
		tokio::spawn(async move {
			if let Err(error) = connection.await {
				tracing::trace!(%error, "envd connection ended");
			}
		});
		let exchange = async {
			let response = sender.send_request(request).await?;
			let (parts, body) = response.into_parts();
			let body = body.collect().await?.to_bytes();
			Ok::<_, hyper::Error>(Response::from_parts(parts, body))
		};
		match tokio::time::timeout(timeout, exchange).await {
			Ok(Ok(response)) => Ok(response),
			Ok(Err(error))
				if error.is_canceled() || error.is_closed() || error.is_incomplete_message() =>
			{
				Err(TryError::Connect(error.to_string()))
			},
			Ok(Err(error)) => Err(TryError::Request(error.into())),
			Err(_) => Err(TryError::Request(anyhow!("envd request timed out after {timeout:?}"))),
		}
	}

	fn builder(&self, method: Method, path: &str) -> http::request::Builder {
		let mut builder = Request::builder()
			.method(method)
			.uri(path)
			.header(header::HOST, format!("127.0.0.1:{ENVD_PORT}"));
		if let Some(token) = &self.token {
			builder = builder.header("X-Access-Token", token);
		}
		builder
	}

	/// One request over a fresh tunnel.
	pub async fn request(&self, request: Request<Full<Bytes>>) -> anyhow::Result<Response<Bytes>> {
		match self
			.exchange(request, REQUEST_TIMEOUT, REQUEST_TIMEOUT)
			.await
		{
			Ok(response) => Ok(response),
			Err(TryError::Connect(message)) => bail!("envd unreachable: {message}"),
			Err(TryError::Request(error)) => Err(error),
		}
	}

	/// `POST /init`, retried every 5 ms (50 ms connect budget per try, 10 s
	/// total) with a fresh timestamp each try; requires `204`. Returns the
	/// `X-Envd-Version` header.
	pub async fn init(&self, init: &GuestInit) -> anyhow::Result<Option<String>> {
		let deadline = Instant::now() + INIT_BUDGET;
		let mut last_error = String::from("no attempt made");
		while Instant::now() < deadline {
			let request = self
				.builder(Method::POST, "/init")
				.header(header::CONTENT_TYPE, "application/json")
				.body(Full::new(init_body(init)))?;
			let remaining = deadline.saturating_duration_since(Instant::now());
			match self
				.exchange(request, INIT_TRY.min(remaining), remaining)
				.await
			{
				Ok(response) if response.status() == StatusCode::NO_CONTENT => {
					return Ok(response
						.headers()
						.get("x-envd-version")
						.and_then(|value| value.to_str().ok())
						.map(str::to_owned));
				},
				Ok(response) => {
					bail!(
						"envd /init returned {}: {}",
						response.status(),
						String::from_utf8_lossy(response.body())
					);
				},
				Err(TryError::Connect(message)) => last_error = message,
				Err(TryError::Request(error)) => return Err(error.context("envd /init")),
			}
			tokio::time::sleep(INIT_RETRY).await;
		}
		bail!("envd /init did not succeed within {INIT_BUDGET:?}: {last_error}")
	}

	/// `GET /health` must answer `204`.
	pub async fn health(&self) -> anyhow::Result<()> {
		let response = self
			.request(self.builder(Method::GET, "/health").body(Full::default())?)
			.await?;
		if response.status() != StatusCode::NO_CONTENT {
			bail!("envd /health returned {}", response.status());
		}
		Ok(())
	}

	/// `GET /metrics`.
	pub async fn metrics(&self) -> anyhow::Result<EnvdMetrics> {
		let response = self
			.request(
				self
					.builder(Method::GET, "/metrics")
					.body(Full::default())?,
			)
			.await?;
		if !response.status().is_success() {
			bail!("envd /metrics returned {}", response.status());
		}
		serde_json::from_slice(response.body()).context("malformed envd /metrics body")
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn init_body_omits_unset_defaults_and_stamps_time() {
		let init = GuestInit {
			env_vars:          [("A".to_owned(), "1".to_owned())].into(),
			envd_access_token: "tok".into(),
			default_user:      Some("user".into()),
			default_workdir:   None,
			sandbox_id:        "isbx".into(),
			template_id:       "tpl".into(),
			ca_bundle:         None,
		};
		let body: serde_json::Value = serde_json::from_slice(&init_body(&init)).unwrap();
		assert_eq!(body["envVars"]["A"], "1");
		assert_eq!(body["accessToken"], "tok");
		assert_eq!(body["defaultUser"], "user");
		assert!(body.get("defaultWorkdir").is_none());
		assert!(body.get("caBundle").is_none());
		let stamp = body["timestamp"].as_str().unwrap();
		assert!(chrono::DateTime::parse_from_rfc3339(stamp).is_ok(), "{stamp}");
	}
}
