//! CLI login (ARCHITECTURE.md §9.2) and the removed `/access-tokens` routes.
//!
//! `GET /docs/api/cli` serves a login page; the email + password form POST
//! redirects to the CLI's localhost callback with OAuth-style tokens.
//! `POST /auth/token` (refresh grant with rotation), `POST /auth/revoke`.

use axum::{
	Form, Json, Router,
	extract::{Query, State},
	http::{StatusCode, header},
	response::{Html, IntoResponse, Response},
	routing::{delete, get, post},
};
use chrono::{Duration, Utc};
use serde::{Deserialize, Serialize};

use crate::{
	crypto,
	error::{ApiError, ApiResult},
	state::AppState,
	store::{self, Rotation},
};

/// Access token lifetime.
pub const ACCESS_TOKEN_TTL_SECS: i64 = 3600;
/// Refresh token lifetime.
pub const REFRESH_TOKEN_TTL_DAYS: i64 = 30;
/// OAuth client id handed to the CLI.
pub const CLIENT_ID: &str = "vmon-cli";
/// Upstream message for the removed access-token routes.
const ACCESS_TOKENS_GONE: &str = "E2B_ACCESS_TOKEN is deprecated and no longer supported. Use an API key (E2B_API_KEY) \
	instead. See https://e2b.dev/docs/migration/access-token-deprecation";

/// Routes of this module.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/docs/api/cli", get(login_page).post(login_submit))
		.route("/auth/token", post(token))
		.route("/auth/revoke", post(revoke))
		.route("/access-tokens", post(access_tokens_gone))
		.route("/access-tokens/{id}", delete(access_tokens_gone))
}

async fn access_tokens_gone() -> ApiError {
	ApiError::gone(ACCESS_TOKENS_GONE)
}

#[derive(Deserialize)]
struct PageQuery {
	next:  Option<String>,
	state: Option<String>,
	email: Option<String>,
	error: Option<String>,
}

#[derive(Deserialize)]
struct LoginForm {
	email:    String,
	password: String,
	next:     String,
}

/// Minimal HTML escaping for attribute and text contexts.
fn escape(text: &str) -> String {
	let mut out = String::with_capacity(text.len());
	for ch in text.chars() {
		match ch {
			'&' => out.push_str("&amp;"),
			'<' => out.push_str("&lt;"),
			'>' => out.push_str("&gt;"),
			'"' => out.push_str("&quot;"),
			'\'' => out.push_str("&#39;"),
			other => out.push(other),
		}
	}
	out
}

fn page(title: &str, body: &str) -> String {
	format!(
		"<!doctype html><html><head><meta \
		 charset=\"utf-8\"><title>{title}</title><style>body{{font-family:system-ui,sans-serif;\
		 max-width:24rem;margin:4rem auto}}input{{display:block;width:100%;margin:.4rem 0 \
		 1rem;padding:.4rem}}.err{{color:#b00}}</style></head><body><h1>{title}</h1>{body}</body></\
		 html>"
	)
}

fn login_form(next: &str, email: &str, error: Option<&str>) -> String {
	let error = error
		.map(|e| format!("<p class=\"err\">{}</p>", escape(e)))
		.unwrap_or_default();
	page(
		"Sign in to the E2B CLI",
		&format!(
			"{error}<form method=\"post\" action=\"/docs/api/cli\"><input type=\"hidden\" \
			 name=\"next\" value=\"{}\"><label>Email<input name=\"email\" type=\"email\" \
			 autocomplete=\"username\" value=\"{}\" required></label><label>Password<input \
			 name=\"password\" type=\"password\" autocomplete=\"current-password\" \
			 required></label><button type=\"submit\">Sign in</button></form>",
			escape(next),
			escape(email)
		),
	)
}

/// Only loopback callbacks may receive tokens (the CLI listens on
/// `localhost:<port>`).
pub fn valid_callback(next: &str) -> Option<url::Url> {
	let url = url::Url::parse(next).ok()?;
	let loopback = matches!(url.host_str()?, "localhost" | "127.0.0.1" | "[::1]");
	(url.scheme() == "http" && loopback).then_some(url)
}

async fn login_page(Query(query): Query<PageQuery>) -> Response {
	if query.state.as_deref() == Some("success") {
		let who = query.email.as_deref().map(escape).unwrap_or_default();
		return Html(page(
			"Signed in",
			&format!("<p>The CLI is now signed in as {who}. You can close this window.</p>"),
		))
		.into_response();
	}
	if let Some(error) = &query.error {
		return Html(page("Sign-in failed", &format!("<p class=\"err\">{}</p>", escape(error))))
			.into_response();
	}
	match query
		.next
		.as_deref()
		.filter(|next| valid_callback(next).is_some())
	{
		Some(next) => Html(login_form(next, "", None)).into_response(),
		None => (
			StatusCode::BAD_REQUEST,
			Html(page(
				"Invalid request",
				"<p>Start the sign-in from the CLI with <code>e2b auth login</code>.</p>",
			)),
		)
			.into_response(),
	}
}

fn base_url(state: &AppState) -> &str {
	state.config.public_url.trim_end_matches('/')
}

/// Issues an access token and a stored refresh token for `user_id`.
async fn issue_tokens(
	state: &AppState,
	user_id: &str,
	email: &str,
) -> store::Result<(String, String)> {
	let access = state
		.keys
		.access_token(user_id, email, ACCESS_TOKEN_TTL_SECS);
	let refresh = crypto::generate_refresh_token();
	let expires = Utc::now() + Duration::days(REFRESH_TOKEN_TTL_DAYS);
	state
		.store
		.insert_refresh_token(user_id, &crypto::sha256_hex(refresh.as_bytes()), expires)
		.await?;
	Ok((access, refresh))
}

async fn login_submit(
	State(state): State<AppState>,
	Form(form): Form<LoginForm>,
) -> ApiResult<Response> {
	let Some(mut callback) = valid_callback(&form.next) else {
		return Ok((
			StatusCode::BAD_REQUEST,
			Html(page("Invalid request", "<p>Invalid CLI callback.</p>")),
		)
			.into_response());
	};
	let user = state.store.user_by_email(&form.email).await?;
	let Some(user) = user.filter(|u| crypto::verify_password(&form.password, &u.password_hash))
	else {
		return Ok((
			StatusCode::UNAUTHORIZED,
			Html(login_form(&form.next, &form.email, Some("Invalid email or password."))),
		)
			.into_response());
	};
	let (access, refresh) = issue_tokens(&state, &user.id, &user.email).await?;
	let base = base_url(&state);
	callback
		.query_pairs_mut()
		.append_pair("email", &user.email)
		.append_pair("accessToken", &access)
		.append_pair("refreshToken", &refresh)
		.append_pair("tokenEndpoint", &format!("{base}/auth/token"))
		.append_pair("revokeEndpoint", &format!("{base}/auth/revoke"))
		.append_pair("clientId", CLIENT_ID);
	Ok((StatusCode::FOUND, [(header::LOCATION, callback.to_string())]).into_response())
}

#[derive(Deserialize)]
struct TokenForm {
	grant_type:    String,
	refresh_token: Option<String>,
}

#[derive(Serialize)]
struct TokenResponse {
	access_token:  String,
	token_type:    &'static str,
	expires_in:    i64,
	refresh_token: String,
}

/// RFC 6749 §5.2 error body.
fn oauth_error(status: StatusCode, error: &'static str, description: &str) -> Response {
	(status, Json(serde_json::json!({"error": error, "error_description": description})))
		.into_response()
}

/// `POST /auth/token` (`grant_type=refresh_token`): rotates the refresh
/// token. Reusing a rotated token revokes the user's whole token family.
async fn token(State(state): State<AppState>, Form(form): Form<TokenForm>) -> ApiResult<Response> {
	if form.grant_type != "refresh_token" {
		return Ok(oauth_error(
			StatusCode::BAD_REQUEST,
			"unsupported_grant_type",
			"only refresh_token is supported",
		));
	}
	let Some(old) = form.refresh_token.filter(|t| !t.is_empty()) else {
		return Ok(oauth_error(
			StatusCode::BAD_REQUEST,
			"invalid_request",
			"refresh_token is required",
		));
	};
	let refresh = crypto::generate_refresh_token();
	let expires = Utc::now() + Duration::days(REFRESH_TOKEN_TTL_DAYS);
	let rotation = state
		.store
		.rotate_refresh_token(
			&crypto::sha256_hex(old.as_bytes()),
			&crypto::sha256_hex(refresh.as_bytes()),
			expires,
		)
		.await?;
	let user_id = match rotation {
		Rotation::Rotated { user_id } => user_id,
		Rotation::Invalid | Rotation::Reused => {
			return Ok(oauth_error(
				StatusCode::BAD_REQUEST,
				"invalid_grant",
				"refresh token is invalid or expired",
			));
		},
	};
	let Some(user) = state.store.user_by_id(&user_id).await? else {
		return Ok(oauth_error(StatusCode::BAD_REQUEST, "invalid_grant", "user no longer exists"));
	};
	Ok(Json(TokenResponse {
		access_token:  state
			.keys
			.access_token(&user.id, &user.email, ACCESS_TOKEN_TTL_SECS),
		token_type:    "Bearer",
		expires_in:    ACCESS_TOKEN_TTL_SECS,
		refresh_token: refresh,
	})
	.into_response())
}

#[derive(Deserialize)]
struct RevokeForm {
	token: Option<String>,
}

/// `POST /auth/revoke` (RFC 7009): always 200, revokes the refresh token
/// when it is known.
async fn revoke(
	State(state): State<AppState>,
	Form(form): Form<RevokeForm>,
) -> ApiResult<StatusCode> {
	if let Some(token) = form.token.filter(|t| !t.is_empty()) {
		state
			.store
			.revoke_refresh_token(&crypto::sha256_hex(token.as_bytes()))
			.await?;
	}
	Ok(StatusCode::OK)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn only_loopback_http_callbacks_are_accepted() {
		assert!(valid_callback("http://localhost:4567").is_some());
		assert!(valid_callback("http://127.0.0.1:4567/cb").is_some());
		assert!(valid_callback("https://localhost:4567").is_none());
		assert!(valid_callback("http://evil.example:4567").is_none());
		assert!(valid_callback("http://localhost.evil.example").is_none());
		assert!(valid_callback("not a url").is_none());
	}

	#[test]
	fn form_escapes_reflected_values() {
		let html = login_form("http://localhost:1/\"><script>", "<b>", Some("<x>"));
		assert!(!html.contains("<script>"));
		assert!(!html.contains("<b>"));
		assert!(html.contains("&lt;x&gt;"));
	}
}
