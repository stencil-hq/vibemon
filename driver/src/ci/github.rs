//! GitHub REST client of vmon CI: app JWTs (RS256), cached installation
//! tokens, JIT runner configs, runners, jobs, runs, app webhook deliveries,
//! installations, users, and manifest conversions.

use std::{collections::BTreeMap, fmt, time::Duration};

use chrono::{DateTime, Utc};
use jsonwebtoken::{Algorithm, EncodingKey, Header};
use reqwest::{Method, StatusCode};
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::{Value, json};

use crate::{error::ApiError, state::AppState, store::ci::CiAppRow};

/// Per-request timeout.
const TIMEOUT: Duration = Duration::from_secs(30);
/// Pages walked by a list call (100 items each).
const MAX_PAGES: u32 = 20;
/// Installation tokens are renewed this long before they expire.
const TOKEN_MARGIN: chrono::Duration = chrono::Duration::minutes(5);
const API_VERSION: &str = "2022-11-28";

/// A failed GitHub call.
#[derive(Clone, Debug)]
pub struct GithubError {
	/// HTTP status (`None`: no answer).
	pub status:       Option<u16>,
	pub message:      String,
	/// Primary or secondary rate limit.
	pub rate_limited: bool,
}

impl GithubError {
	fn transport(error: &reqwest::Error) -> Self {
		Self { status: None, message: error.to_string(), rate_limited: false }
	}

	/// Worth retrying later: no answer, a server error, or a rate limit.
	pub fn is_transient(&self) -> bool {
		self.rate_limited || self.status.is_none_or(|status| status >= 500)
	}

	pub fn is(&self, status: StatusCode) -> bool {
		self.status == Some(status.as_u16())
	}
}

impl fmt::Display for GithubError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self.status {
			Some(status) => write!(f, "GitHub answered {status}: {}", self.message),
			None => write!(f, "GitHub unreachable: {}", self.message),
		}
	}
}

impl From<GithubError> for ApiError {
	fn from(error: GithubError) -> Self {
		Self::new(StatusCode::BAD_GATEWAY, error.to_string())
	}
}

pub type Result<T, E = GithubError> = std::result::Result<T, E>;

/// Who a call authenticates as.
#[derive(Clone, Copy, Debug)]
enum Auth {
	/// The app (JWT).
	App,
	/// An installation (installation token).
	Installation(u64),
}

#[derive(Debug, Deserialize)]
pub struct Account {
	pub login: String,
	#[serde(rename = "type", default)]
	pub kind:  String,
}

/// `GET /app`.
#[derive(Debug, Deserialize)]
pub struct AppInfo {
	pub id:          u64,
	pub slug:        String,
	pub name:        String,
	pub owner:       Account,
	pub html_url:    String,
	/// Permission → `read`/`write` the app requests.
	#[serde(default)]
	pub permissions: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
pub struct Installation {
	pub id:           u64,
	pub account:      Account,
	#[serde(default)]
	pub suspended_at: Option<String>,
	/// Permission → `read`/`write` the installation granted.
	#[serde(default)]
	pub permissions:  BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct Repository {
	pub full_name: String,
	#[serde(default)]
	pub private:   bool,
}

#[derive(Debug, Deserialize)]
pub struct Login {
	pub login: String,
}

/// `GET /repos/{repo}/actions/runs/{id}` (the fields CI reads).
#[derive(Debug, Deserialize)]
pub struct Run {
	pub actor:            Login,
	pub triggering_actor: Option<Login>,
}

/// A workflow job as the webhooks and the jobs API carry it.
#[derive(Clone, Debug, Deserialize)]
pub struct WorkflowJob {
	pub id:            u64,
	pub run_id:        u64,
	#[serde(default)]
	pub run_attempt:   u32,
	#[serde(default)]
	pub workflow_name: Option<String>,
	pub name:          String,
	#[serde(default)]
	pub head_branch:   Option<String>,
	#[serde(default)]
	pub head_sha:      String,
	#[serde(default)]
	pub html_url:      String,
	#[serde(default)]
	pub labels:        Vec<String>,
	pub status:        String,
	#[serde(default)]
	pub conclusion:    Option<String>,
	#[serde(default)]
	pub runner_name:   Option<String>,
	#[serde(default)]
	pub created_at:    Option<DateTime<Utc>>,
	#[serde(default)]
	pub started_at:    Option<DateTime<Utc>>,
	#[serde(default)]
	pub completed_at:  Option<DateTime<Utc>>,
}

#[derive(Debug, Deserialize)]
pub struct Runner {
	pub id:     u64,
	pub name:   String,
	#[serde(default)]
	pub status: String,
	#[serde(default)]
	pub busy:   bool,
}

/// `POST …/generate-jitconfig`.
#[derive(Deserialize)]
pub struct JitConfig {
	pub runner:             Runner,
	/// The runner's credentials: passed to the runner, never stored.
	pub encoded_jit_config: String,
}

/// One app webhook delivery attempt.
#[derive(Debug, Deserialize)]
pub struct Delivery {
	pub id:           u64,
	pub guid:         String,
	pub delivered_at: DateTime<Utc>,
	#[serde(default)]
	pub redelivery:   bool,
	pub status_code:  u16,
	pub event:        String,
	#[serde(default)]
	pub action:       Option<String>,
}

/// `POST /app-manifests/{code}/conversions`.
#[derive(Deserialize)]
pub struct Conversion {
	pub id:             u64,
	pub slug:           String,
	pub name:           String,
	pub owner:          Account,
	pub html_url:       String,
	#[serde(default)]
	pub client_id:      Option<String>,
	#[serde(default)]
	pub client_secret:  Option<String>,
	pub webhook_secret: String,
	pub pem:            String,
	#[serde(default)]
	pub permissions:    BTreeMap<String, String>,
}

#[derive(Serialize)]
struct Claims {
	iat: i64,
	exp: i64,
	iss: String,
}

/// Web base URL (`https://github.com`) of an API base URL.
pub fn web_url(api: &str) -> String {
	let api = api.trim_end_matches('/');
	if api == "https://api.github.com" {
		return "https://github.com".to_owned();
	}
	api.strip_suffix("/api/v3").unwrap_or(api).to_owned()
}

/// Client acting as the configured app.
pub struct Github {
	state:  AppState,
	app_id: u64,
	key:    EncodingKey,
}

impl Github {
	/// Client of `app` (decrypts its private key).
	pub fn new(state: &AppState, app: &CiAppRow) -> Result<Self> {
		let pem = state
			.keys
			.decrypt(&app.private_key_enc)
			.map_err(|error| GithubError {
				status:       None,
				message:      format!("the stored private key cannot be decrypted: {error}"),
				rate_limited: false,
			})?;
		Self::with_key(state, app.app_id, &pem)
	}

	/// Client of app `app_id` signing with PEM `pem` (PKCS#1 or PKCS#8).
	pub fn with_key(state: &AppState, app_id: u64, pem: &[u8]) -> Result<Self> {
		let key = EncodingKey::from_rsa_pem(pem).map_err(|error| GithubError {
			status:       None,
			message:      format!("the private key is not an RSA PEM key: {error}"),
			rate_limited: false,
		})?;
		Ok(Self { state: state.clone(), app_id, key })
	}

	/// Client of the stored app; `None` when none is configured.
	pub async fn load(state: &AppState) -> Result<Option<Self>, ApiError> {
		match state.store.ci_app().await? {
			Some(app) => Ok(Some(Self::new(state, &app)?)),
			None => Ok(None),
		}
	}

	fn api(&self) -> &str {
		self.state.config.ci_github_api.trim_end_matches('/')
	}

	/// A fresh app JWT (valid 9 minutes, backdated 1 minute for clock skew).
	fn jwt(&self) -> Result<String> {
		let now = Utc::now().timestamp();
		let claims = Claims { iat: now - 60, exp: now + 9 * 60, iss: self.app_id.to_string() };
		jsonwebtoken::encode(&Header::new(Algorithm::RS256), &claims, &self.key).map_err(|error| {
			GithubError {
				status:       None,
				message:      format!("signing the app JWT: {error}"),
				rate_limited: false,
			}
		})
	}

	/// Installation token, cached until shortly before it expires.
	async fn installation_token(&self, installation_id: u64) -> Result<String> {
		let cache_key = (self.app_id, installation_id);
		if let Some((token, expires)) = self.state.ci.tokens.lock().get(&cache_key)
			&& *expires - TOKEN_MARGIN > Utc::now()
		{
			return Ok(token.clone());
		}
		#[derive(Deserialize)]
		struct Token {
			token:      String,
			expires_at: DateTime<Utc>,
		}
		// Authenticated as the app directly: `send` itself asks for tokens.
		let path = format!("/app/installations/{installation_id}/access_tokens");
		let request = self
			.state
			.http
			.post(format!("{}{path}", self.api()))
			.bearer_auth(self.jwt()?)
			.json(&json!({}));
		let token: Token = send(request)
			.await?
			.json()
			.await
			.map_err(|error| GithubError {
				status:       None,
				message:      format!("unexpected reply to {path}: {error}"),
				rate_limited: false,
			})?;
		self
			.state
			.ci
			.tokens
			.lock()
			.insert(cache_key, (token.token.clone(), token.expires_at));
		Ok(token.token)
	}

	async fn send(
		&self,
		auth: Auth,
		method: Method,
		path: &str,
		body: Option<&Value>,
	) -> Result<reqwest::Response> {
		let bearer = match auth {
			Auth::App => self.jwt()?,
			Auth::Installation(id) => self.installation_token(id).await?,
		};
		let mut request = self
			.state
			.http
			.request(method, format!("{}{path}", self.api()))
			.bearer_auth(bearer);
		if let Some(body) = body {
			request = request.json(body);
		}
		send(request).await
	}

	async fn json<T: DeserializeOwned>(
		&self,
		auth: Auth,
		method: Method,
		path: &str,
		body: Option<&Value>,
	) -> Result<T> {
		let response = self.send(auth, method, path, body).await?;
		response.json().await.map_err(|error| GithubError {
			status:       None,
			message:      format!("unexpected reply to {path}: {error}"),
			rate_limited: false,
		})
	}

	/// Every item of a paginated list; `field` names the array inside the
	/// reply object (`None`: the reply is the array).
	async fn list<T: DeserializeOwned>(
		&self,
		auth: Auth,
		path: &str,
		field: Option<&str>,
	) -> Result<Vec<T>> {
		let mut items = Vec::new();
		let separator = if path.contains('?') { '&' } else { '?' };
		for page in 1..=MAX_PAGES {
			let reply: Value = self
				.json(auth, Method::GET, &format!("{path}{separator}per_page=100&page={page}"), None)
				.await?;
			let array = match field {
				Some(field) => reply.get(field).cloned().unwrap_or(Value::Null),
				None => reply,
			};
			let batch: Vec<T> = serde_json::from_value(array).map_err(|error| GithubError {
				status:       None,
				message:      format!("unexpected reply to {path}: {error}"),
				rate_limited: false,
			})?;
			let done = batch.len() < 100;
			items.extend(batch);
			if done {
				break;
			}
		}
		Ok(items)
	}

	// ------------------------------------------------------------- app

	pub async fn app(&self) -> Result<AppInfo> {
		self.json(Auth::App, Method::GET, "/app", None).await
	}

	/// The app's webhook URL (`GET /app/hook/config`).
	pub async fn webhook_url(&self) -> Result<Option<String>> {
		#[derive(Deserialize)]
		struct HookConfig {
			url: Option<String>,
		}
		let config: HookConfig = self
			.json(Auth::App, Method::GET, "/app/hook/config", None)
			.await?;
		Ok(config.url.filter(|url| !url.is_empty()))
	}

	pub async fn installations(&self) -> Result<Vec<Installation>> {
		self.list(Auth::App, "/app/installations", None).await
	}

	pub async fn installation(&self, installation_id: u64) -> Result<Installation> {
		self
			.json(Auth::App, Method::GET, &format!("/app/installations/{installation_id}"), None)
			.await
	}

	/// Repositories an installation grants.
	pub async fn installation_repos(&self, installation_id: u64) -> Result<Vec<Repository>> {
		self
			.list(
				Auth::Installation(installation_id),
				"/installation/repositories",
				Some("repositories"),
			)
			.await
	}

	/// Whether GitHub user `login` exists.
	pub async fn user_exists(&self, installation_id: u64, login: &str) -> Result<bool> {
		match self
			.send(Auth::Installation(installation_id), Method::GET, &format!("/users/{login}"), None)
			.await
		{
			Ok(_) => Ok(true),
			Err(error) if error.is(StatusCode::NOT_FOUND) => Ok(false),
			Err(error) => Err(error),
		}
	}

	/// Failed app webhook deliveries (`status=failure`, filtered by GitHub),
	/// newest first, delivered at or after `since`: follows the `Link`
	/// `rel="next"` cursor until a page reaches past `since`, at most
	/// `max_pages` pages.
	pub async fn failed_deliveries(
		&self,
		since: DateTime<Utc>,
		max_pages: u32,
	) -> Result<Vec<Delivery>> {
		let mut path = "/app/hook/deliveries?per_page=100&status=failure".to_owned();
		let mut deliveries = Vec::new();
		for _ in 0..max_pages {
			let response = self.send(Auth::App, Method::GET, &path, None).await?;
			let next = response
				.headers()
				.get(reqwest::header::LINK)
				.and_then(|value| value.to_str().ok())
				.and_then(next_link)
				.and_then(|url| url.strip_prefix(self.api()).map(str::to_owned));
			let page: Vec<Delivery> = response.json().await.map_err(|error| GithubError {
				status:       None,
				message:      format!("unexpected reply to {path}: {error}"),
				rate_limited: false,
			})?;
			let past = page.iter().any(|delivery| delivery.delivered_at < since);
			deliveries.extend(
				page
					.into_iter()
					.filter(|delivery| delivery.delivered_at >= since),
			);
			match next {
				Some(next) if !past => path = next,
				_ => break,
			}
		}
		Ok(deliveries)
	}

	pub async fn redeliver(&self, delivery_id: u64) -> Result<()> {
		self
			.send(
				Auth::App,
				Method::POST,
				&format!("/app/hook/deliveries/{delivery_id}/attempts"),
				None,
			)
			.await
			.map(drop)
	}

	// ----------------------------------------------------------- actions

	pub async fn run(&self, installation_id: u64, repo: &str, run_id: u64) -> Result<Run> {
		self
			.json(
				Auth::Installation(installation_id),
				Method::GET,
				&format!("/repos/{repo}/actions/runs/{run_id}"),
				None,
			)
			.await
	}

	pub async fn job(&self, installation_id: u64, repo: &str, job_id: u64) -> Result<WorkflowJob> {
		self
			.json(
				Auth::Installation(installation_id),
				Method::GET,
				&format!("/repos/{repo}/actions/jobs/{job_id}"),
				None,
			)
			.await
	}

	/// A finished job's log (GitHub redirects to the download).
	pub async fn job_logs(&self, installation_id: u64, repo: &str, job_id: u64) -> Result<String> {
		let response = self
			.send(
				Auth::Installation(installation_id),
				Method::GET,
				&format!("/repos/{repo}/actions/jobs/{job_id}/logs"),
				None,
			)
			.await?;
		response
			.text()
			.await
			.map_err(|error| GithubError::transport(&error))
	}

	/// Registers JIT runner `name` (labels `labels`, default runner group).
	pub async fn generate_jitconfig(
		&self,
		installation_id: u64,
		repo: &str,
		name: &str,
		labels: &[String],
	) -> Result<JitConfig> {
		let body = json!({
			"name": name,
			"runner_group_id": 1,
			"labels": labels,
			"work_folder": "_work",
		});
		self
			.json(
				Auth::Installation(installation_id),
				Method::POST,
				&format!("/repos/{repo}/actions/runners/generate-jitconfig"),
				Some(&body),
			)
			.await
	}

	pub async fn runners(&self, installation_id: u64, repo: &str) -> Result<Vec<Runner>> {
		self
			.list(
				Auth::Installation(installation_id),
				&format!("/repos/{repo}/actions/runners"),
				Some("runners"),
			)
			.await
	}

	pub async fn runner(&self, installation_id: u64, repo: &str, runner_id: u64) -> Result<Runner> {
		self
			.json(
				Auth::Installation(installation_id),
				Method::GET,
				&format!("/repos/{repo}/actions/runners/{runner_id}"),
				None,
			)
			.await
	}

	/// Removes a runner; an unknown runner is no error.
	pub async fn delete_runner(
		&self,
		installation_id: u64,
		repo: &str,
		runner_id: u64,
	) -> Result<()> {
		match self
			.send(
				Auth::Installation(installation_id),
				Method::DELETE,
				&format!("/repos/{repo}/actions/runners/{runner_id}"),
				None,
			)
			.await
		{
			Ok(_) => Ok(()),
			Err(error) if error.is(StatusCode::NOT_FOUND) => Ok(()),
			Err(error) => Err(error),
		}
	}
}

/// URL of the `rel="next"` entry of a `Link` header.
fn next_link(header: &str) -> Option<&str> {
	header.split(',').find_map(|entry| {
		let (url, params) = entry.split_once(';')?;
		params
			.split(';')
			.any(|param| param.trim() == r#"rel="next""#)
			.then(|| url.trim().trim_start_matches('<').trim_end_matches('>'))
	})
}

/// Exchanges a manifest flow's `code` for the new app's credentials
/// (unauthenticated).
pub async fn convert_manifest(state: &AppState, code: &str) -> Result<Conversion> {
	let api = state.config.ci_github_api.trim_end_matches('/');
	let request = state
		.http
		.post(format!("{api}/app-manifests/{code}/conversions"));
	let response = send(request).await?;
	response.json().await.map_err(|error| GithubError {
		status:       None,
		message:      format!("unexpected manifest conversion reply: {error}"),
		rate_limited: false,
	})
}

async fn send(request: reqwest::RequestBuilder) -> Result<reqwest::Response> {
	let response = request
		.header("Accept", "application/vnd.github+json")
		.header("X-GitHub-Api-Version", API_VERSION)
		.header("User-Agent", "vmon-ci")
		.timeout(TIMEOUT)
		.send()
		.await
		.map_err(|error| GithubError::transport(&error))?;
	let status = response.status();
	if status.is_success() {
		return Ok(response);
	}
	let exhausted = response
		.headers()
		.get("x-ratelimit-remaining")
		.is_some_and(|remaining| remaining.as_bytes() == b"0");
	let retry_after = response.headers().contains_key("retry-after");
	let body = response.text().await.unwrap_or_default();
	let message = serde_json::from_str::<Value>(&body)
		.ok()
		.and_then(|value| {
			value
				.get("message")
				.and_then(Value::as_str)
				.map(str::to_owned)
		})
		.unwrap_or_else(|| body.chars().take(300).collect());
	let rate_limited = status == StatusCode::TOO_MANY_REQUESTS
		|| (status == StatusCode::FORBIDDEN && (exhausted || retry_after));
	Err(GithubError { status: Some(status.as_u16()), message, rate_limited })
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn next_links() {
		let header = r#"<https://api.github.com/app/hook/deliveries?per_page=100&cursor=v1_9>; rel="next", <https://api.github.com/app/hook/deliveries?per_page=100>; rel="first""#;
		assert_eq!(
			next_link(header),
			Some("https://api.github.com/app/hook/deliveries?per_page=100&cursor=v1_9")
		);
		assert_eq!(next_link(r#"<https://x/y>; rel="prev""#), None);
	}

	#[test]
	fn web_urls_of_api_urls() {
		assert_eq!(web_url("https://api.github.com/"), "https://github.com");
		assert_eq!(web_url("https://ghe.example.com/api/v3"), "https://ghe.example.com");
		assert_eq!(web_url("http://127.0.0.1:9"), "http://127.0.0.1:9");
	}
}
