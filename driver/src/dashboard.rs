//! Admin dashboard at `/dashboard/` on the public listeners.
//!
//! The React app from `ui/` (`just ui`) is built into `driver/web/` and
//! embedded in the binary. The page is public; every API call it makes
//! carries the operator's `X-Admin-Token`, and sandbox terminals/files go
//! through the normal ingress with the sandbox's envd token.

use std::sync::LazyLock;

use axum::{
	Router,
	http::{HeaderMap, HeaderValue, StatusCode, header},
	response::{IntoResponse, Redirect, Response},
	routing::get,
};
use sha2::{Digest, Sha256};

/// `style-src 'unsafe-inline'`: the terminal (xterm.js) injects `<style>`
/// elements at runtime. Scripts stay same-origin only.
const CSP: &str = "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; \
                   img-src 'self' data: blob:; connect-src 'self'; object-src 'none'; base-uri \
                   'none'; frame-ancestors 'none'; form-action 'none'";

/// One embedded file with its strong `ETag`.
struct Asset {
	body: &'static [u8],
	mime: &'static str,
	etag: HeaderValue,
}

impl Asset {
	fn new(body: &'static [u8], mime: &'static str) -> Self {
		let digest = Sha256::digest(body);
		let etag = HeaderValue::from_str(&format!("\"{}\"", hex::encode(&digest[..12])))
			.expect("hex ETag is a valid header value");
		Self { body, mime, etag }
	}

	/// 200 with the body, or 304 when `If-None-Match` names the current
	/// `ETag`. `no-cache` makes browsers revalidate, so a new binary takes
	/// effect on the next load despite the fixed file names.
	fn respond(&self, request: &HeaderMap) -> Response {
		let etag = &self.etag;
		let fresh = request
			.get(header::IF_NONE_MATCH)
			.and_then(|v| v.to_str().ok())
			.is_some_and(|v| v.split(',').any(|tag| tag.trim() == etag));
		let headers = [
			(header::CONTENT_TYPE, HeaderValue::from_static(self.mime)),
			(header::ETAG, etag.clone()),
			(header::CACHE_CONTROL, HeaderValue::from_static("no-cache")),
			(header::CONTENT_SECURITY_POLICY, HeaderValue::from_static(CSP)),
			(header::X_CONTENT_TYPE_OPTIONS, HeaderValue::from_static("nosniff")),
			(header::REFERRER_POLICY, HeaderValue::from_static("no-referrer")),
		];
		if fresh {
			(StatusCode::NOT_MODIFIED, headers).into_response()
		} else {
			(headers, self.body).into_response()
		}
	}
}

static INDEX: LazyLock<Asset> =
	LazyLock::new(|| Asset::new(include_bytes!("../web/index.html"), "text/html; charset=utf-8"));
static SCRIPT: LazyLock<Asset> = LazyLock::new(|| {
	Asset::new(include_bytes!("../web/assets/app.js"), "text/javascript; charset=utf-8")
});
static STYLE: LazyLock<Asset> =
	LazyLock::new(|| Asset::new(include_bytes!("../web/assets/app.css"), "text/css; charset=utf-8"));

/// Dashboard routes, merged into the API router by
/// [`crate::Driver::public_router`]. Every non-asset path under `/dashboard/`
/// serves the app shell, which routes client-side; `/` redirects there.
pub fn router<S: Clone + Send + Sync + 'static>() -> Router<S> {
	Router::new()
		.route("/", get(|| async { Redirect::to("/dashboard/") }))
		.route("/dashboard", get(|| async { Redirect::to("/dashboard/") }))
		.route("/dashboard/", get(|headers: HeaderMap| async move { INDEX.respond(&headers) }))
		.route("/dashboard/{*path}", get(|headers: HeaderMap| async move { INDEX.respond(&headers) }))
		.route(
			"/dashboard/assets/app.js",
			get(|headers: HeaderMap| async move { SCRIPT.respond(&headers) }),
		)
		.route(
			"/dashboard/assets/app.css",
			get(|headers: HeaderMap| async move { STYLE.respond(&headers) }),
		)
		.route("/dashboard/assets/{*path}", get(|| async { StatusCode::NOT_FOUND }))
}

#[cfg(test)]
mod tests {
	use axum::{body::Body, http::Request};
	use tower::ServiceExt;

	use super::*;

	async fn get_path(path: &str, if_none_match: Option<&HeaderValue>) -> Response {
		let mut request = Request::get(path);
		if let Some(tag) = if_none_match {
			request = request.header(header::IF_NONE_MATCH, tag);
		}
		router::<()>()
			.oneshot(request.body(Body::empty()).unwrap())
			.await
			.unwrap()
	}

	#[tokio::test]
	async fn deep_links_get_the_shell_but_unknown_assets_do_not() {
		let deep = get_path("/dashboard/sandboxes/iabc?tab=files", None).await;
		assert_eq!(deep.status(), StatusCode::OK);
		assert_eq!(deep.headers()[header::CONTENT_TYPE], "text/html; charset=utf-8");

		let script = get_path("/dashboard/assets/app.js", None).await;
		assert_eq!(script.headers()[header::CONTENT_TYPE], "text/javascript; charset=utf-8");

		assert_eq!(
			get_path("/dashboard/assets/app-old.js", None)
				.await
				.status(),
			StatusCode::NOT_FOUND
		);
		assert_eq!(get_path("/", None).await.status(), StatusCode::SEE_OTHER);
	}

	#[tokio::test]
	async fn matching_etag_revalidates_without_a_body() {
		let first = get_path("/dashboard/", None).await;
		let tag = first.headers()[header::ETAG].clone();
		assert_eq!(get_path("/dashboard/", Some(&tag)).await.status(), StatusCode::NOT_MODIFIED);
		let stale = HeaderValue::from_static("\"stale\"");
		assert_eq!(get_path("/dashboard/", Some(&stale)).await.status(), StatusCode::OK);
	}
}
