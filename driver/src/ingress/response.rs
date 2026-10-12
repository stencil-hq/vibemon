//! Responses synthesized by the ingress proxy (e2b-api Part F §5.3–5.4) and
//! the CORS policy shared with the API (Part H.1).

use axum::{
	body::Body,
	http::{HeaderMap, HeaderValue, Method, StatusCode, header},
	response::{IntoResponse, Response},
};
use serde_json::{Value, json};

/// Request headers browsers may send cross-origin to the API and sandboxes.
const ALLOW_HEADERS: &str =
	"authorization, content-type, x-api-key, x-access-token, x-team-id, idempotency-key, \
	 e2b-sandbox-id, e2b-sandbox-port, e2b-traffic-access-token, connect-protocol-version, \
	 connect-timeout-ms, connect-content-encoding, connect-accept-encoding, \
	 keepalive-ping-interval, lang, lang_version, package_version, publisher, sdk_runtime, system, \
	 browser, range";

/// Response headers exposed to browser JS.
const EXPOSE_HEADERS: &str = "x-next-token, x-total-running, retry-after, content-disposition";

/// A proxy-side failure rendered as JSON (or HTML for browser navigations).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProxyError {
	pub status: StatusCode,
	pub body:   Value,
}

impl ProxyError {
	/// 502: no route and no auto-resume.
	pub fn not_found(sandbox_id: &str) -> Self {
		Self::with_message(StatusCode::BAD_GATEWAY, sandbox_id, "The sandbox was not found")
	}

	/// 502: the sandbox runs but the guest port refused every dial.
	pub fn port_closed(sandbox_id: &str, port: u16) -> Self {
		Self {
			status: StatusCode::BAD_GATEWAY,
			body:   json!({
				"sandboxId": sandbox_id,
				"message": "The sandbox is running but port is not open",
				"port": port,
				"code": 502,
			}),
		}
	}

	/// 403: private sandbox port without a valid traffic token.
	pub fn traffic_denied(sandbox_id: &str, message: &str) -> Self {
		Self::with_message(StatusCode::FORBIDDEN, sandbox_id, message)
	}

	/// 409: the sandbox is pausing/snapshotting past the wait budget.
	pub fn transitioning(sandbox_id: &str) -> Self {
		Self::with_message(
			StatusCode::CONFLICT,
			sandbox_id,
			"The sandbox is still transitioning. Try again in a moment.",
		)
	}

	/// 429: auto-resume hit the team concurrency limit.
	pub fn limit(sandbox_id: &str, message: &str) -> Self {
		Self::with_message(StatusCode::TOO_MANY_REQUESTS, sandbox_id, message)
	}

	/// 403: auto-resume refused for the provided credentials.
	pub fn resume_denied(sandbox_id: &str) -> Self {
		Self::with_message(
			StatusCode::FORBIDDEN,
			sandbox_id,
			"This sandbox can't be resumed with the credentials provided. Check your access token \
			 and try again.",
		)
	}

	/// 404: reserved envd endpoint requested through the public URL.
	pub fn reserved_path(path: &str) -> Self {
		Self {
			status: StatusCode::NOT_FOUND,
			body:   json!({
				"message": "This endpoint is reserved for the E2B control plane and is not reachable through the sandbox URL",
				"path": path,
				"code": 404,
			}),
		}
	}

	fn with_message(status: StatusCode, sandbox_id: &str, message: &str) -> Self {
		Self {
			status,
			body: json!({ "sandboxId": sandbox_id, "message": message, "code": status.as_u16() }),
		}
	}

	/// Renders for the requesting client: HTML only for browser navigations.
	pub fn render(&self, request_headers: &HeaderMap) -> Response {
		let mut response = if wants_html(request_headers) {
			let message = self
				.body
				.get("message")
				.and_then(Value::as_str)
				.unwrap_or_default();
			let html = format!(
				"<!doctype html><html><head><meta \
				 charset=\"utf-8\"><title>{code}</title></head><body><h1>{code}</h1><p>{}</p></body></\
				 html>",
				escape_html(message),
				code = self.status.as_u16()
			);
			(self.status, [(header::CONTENT_TYPE, "text/html; charset=utf-8")], html).into_response()
		} else {
			(
				self.status,
				[(header::CONTENT_TYPE, "application/json; charset=utf-8")],
				self.body.to_string(),
			)
				.into_response()
		};
		allow_any_origin(response.headers_mut());
		response
	}
}

/// Plain-text 400 for malformed routing (upstream `http.Error`).
pub fn bad_request(message: &str) -> Response {
	plain(StatusCode::BAD_REQUEST, format!("{message}\n"))
}

/// Plain-text 500 for unexpected routing failures.
pub fn routing_failed(message: &str) -> Response {
	plain(
		StatusCode::INTERNAL_SERVER_ERROR,
		format!("Unexpected error when routing request: {message}\n"),
	)
}

fn plain(status: StatusCode, text: String) -> Response {
	let mut response =
		(status, [(header::CONTENT_TYPE, "text/plain; charset=utf-8")], text).into_response();
	allow_any_origin(response.headers_mut());
	response
}

/// Browser navigation detection (upstream `template.go`): not JSON-preferring,
/// not script-initiated, and a browser User-Agent.
fn wants_html(headers: &HeaderMap) -> bool {
	let get = |name: &str| {
		headers
			.get(name)
			.and_then(|v| v.to_str().ok())
			.unwrap_or_default()
	};
	let accept = get("accept").to_ascii_lowercase();
	if accept.contains("application/json") && !accept.contains("text/html") {
		return false;
	}
	if matches!(get("sec-fetch-mode"), "cors" | "no-cors" | "same-origin" | "websocket")
		|| headers.contains_key("x-requested-with")
	{
		return false;
	}
	let agent = get("user-agent").to_ascii_lowercase();
	["mozilla", "chrome", "safari", "firefox", "edge", "opera", "msie"]
		.iter()
		.any(|b| agent.contains(b))
}

fn escape_html(text: &str) -> String {
	text
		.replace('&', "&amp;")
		.replace('<', "&lt;")
		.replace('>', "&gt;")
		.replace('"', "&quot;")
}

/// Whether the request is a CORS preflight.
pub fn is_preflight(method: &Method, headers: &HeaderMap) -> bool {
	method == Method::OPTIONS
		&& headers.contains_key(header::ORIGIN)
		&& headers.contains_key(header::ACCESS_CONTROL_REQUEST_METHOD)
}

/// 204 preflight answer: any method, the standard SDK headers plus whatever
/// the browser asked for, cached for a day.
pub fn preflight(request_headers: &HeaderMap) -> Response {
	let mut response = Response::new(Body::empty());
	*response.status_mut() = StatusCode::NO_CONTENT;
	let headers = response.headers_mut();
	allow_any_origin(headers);
	headers.insert(header::ACCESS_CONTROL_ALLOW_METHODS, HeaderValue::from_static("*"));
	let allow = match request_headers
		.get(header::ACCESS_CONTROL_REQUEST_HEADERS)
		.and_then(|v| v.to_str().ok())
	{
		Some(requested) if !requested.trim().is_empty() => format!("{ALLOW_HEADERS}, {requested}"),
		_ => ALLOW_HEADERS.to_owned(),
	};
	if let Ok(value) = HeaderValue::from_str(&allow) {
		headers.insert(header::ACCESS_CONTROL_ALLOW_HEADERS, value);
	}
	headers.insert(header::ACCESS_CONTROL_MAX_AGE, HeaderValue::from_static("86400"));
	response
}

/// Adds `Access-Control-Allow-Origin: *` and the exposed headers unless the
/// response already carries its own CORS policy.
pub fn allow_any_origin(headers: &mut HeaderMap) {
	if headers.contains_key(header::ACCESS_CONTROL_ALLOW_ORIGIN) {
		return;
	}
	headers.insert(header::ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
	headers.insert(header::ACCESS_CONTROL_EXPOSE_HEADERS, HeaderValue::from_static(EXPOSE_HEADERS));
}

#[cfg(test)]
mod tests {
	use http_body_util::BodyExt;

	use super::*;

	async fn body_json(response: Response) -> Value {
		let bytes = response.into_body().collect().await.unwrap().to_bytes();
		serde_json::from_slice(&bytes).unwrap()
	}

	#[tokio::test]
	async fn error_body_shapes() {
		let none = HeaderMap::new();
		let response = ProxyError::not_found("iabc").render(&none);
		assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
		assert_eq!(response.headers()[header::ACCESS_CONTROL_ALLOW_ORIGIN], "*");
		assert_eq!(
			body_json(response).await,
			json!({"sandboxId":"iabc","message":"The sandbox was not found","code":502})
		);

		let response = ProxyError::port_closed("iabc", 8080).render(&none);
		assert_eq!(
			body_json(response).await,
			json!({"sandboxId":"iabc","message":"The sandbox is running but port is not open","port":8080,"code":502})
		);

		let response = ProxyError::traffic_denied("iabc", "nope").render(&none);
		assert_eq!(response.status(), StatusCode::FORBIDDEN);
		assert_eq!(body_json(response).await["code"], 403);

		let response = ProxyError::reserved_path("/init").render(&none);
		assert_eq!(response.status(), StatusCode::NOT_FOUND);
		let body = body_json(response).await;
		assert_eq!((body["path"].as_str(), body["code"].as_u64()), (Some("/init"), Some(404)));
	}

	#[test]
	fn html_only_for_browser_navigations() {
		let browser = |extra: &[(&'static str, &'static str)]| {
			let mut h = HeaderMap::new();
			h.insert(header::USER_AGENT, HeaderValue::from_static("Mozilla/5.0 Chrome/130"));
			h.insert(header::ACCEPT, HeaderValue::from_static("text/html,application/xhtml+xml"));
			for (k, v) in extra {
				h.insert(*k, HeaderValue::from_static(v));
			}
			h
		};
		let content_type = |h: &HeaderMap| {
			ProxyError::not_found("i").render(h).headers()[header::CONTENT_TYPE]
				.to_str()
				.unwrap()
				.to_owned()
		};
		assert!(content_type(&browser(&[])).starts_with("text/html"));
		assert!(
			content_type(&browser(&[("sec-fetch-mode", "cors")])).starts_with("application/json")
		);
		assert!(
			content_type(&browser(&[("x-requested-with", "XMLHttpRequest")]))
				.starts_with("application/json")
		);
		assert!(
			content_type(&browser(&[("accept", "application/json")])).starts_with("application/json")
		);
		let mut sdk = HeaderMap::new();
		sdk.insert(header::USER_AGENT, HeaderValue::from_static("e2b-python-sdk/2.3.0"));
		assert!(content_type(&sdk).starts_with("application/json"));
	}

	#[test]
	fn preflight_echoes_requested_headers_and_keeps_existing_cors() {
		let mut req = HeaderMap::new();
		req.insert(header::ACCESS_CONTROL_REQUEST_HEADERS, HeaderValue::from_static("x-custom"));
		let response = preflight(&req);
		assert_eq!(response.status(), StatusCode::NO_CONTENT);
		let allow = response.headers()[header::ACCESS_CONTROL_ALLOW_HEADERS]
			.to_str()
			.unwrap();
		assert!(
			allow.contains("x-api-key")
				&& allow.contains("keepalive-ping-interval")
				&& allow.ends_with("x-custom")
		);

		let mut own = HeaderMap::new();
		own.insert(
			header::ACCESS_CONTROL_ALLOW_ORIGIN,
			HeaderValue::from_static("https://app.example"),
		);
		allow_any_origin(&mut own);
		assert_eq!(own[header::ACCESS_CONTROL_ALLOW_ORIGIN], "https://app.example");
		assert!(!own.contains_key(header::ACCESS_CONTROL_EXPOSE_HEADERS));
	}
}
