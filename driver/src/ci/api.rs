//! The CI admin API (JSON, camelCase), mounted under `/admin/ci`
//! (`X-Admin-Token`) and `/driver/v1/ci` (cluster secret) with identical
//! handlers. The dashboard and `vmon admin ci` are its clients.

use std::collections::BTreeMap;

use axum::{
	Json, Router,
	extract::{Path, Query, State},
	http::{StatusCode, header},
	response::{IntoResponse, Response},
	routing::{get, post, put},
};
use chrono::{DateTime, Duration, SecondsFormat, Utc};
use serde::{Deserialize, Serialize};
use serde_json::json;
use vmon_node::ci_cache::{
	index,
	records::{self, CacheBlobRow, CacheEntryRow, EntryState},
};

use super::{
	controller::RUNTIME_LIMIT,
	github::{self, Github},
	labels::Size,
};
use crate::{
	error::{ApiError, ApiResult},
	state::AppState,
	store::{
		self, Team,
		ci::{
			CiAccountRow, CiAppRow, CiJobRow, CiManifestRow, InstallationSeen, JobState, PublicPolicy,
		},
	},
};

/// Routes relative to the mount point.
pub fn routes() -> Router<AppState> {
	Router::new()
		.route("/", get(overview))
		.route("/jobs", get(jobs))
		.route("/jobs/{job_id}", get(job))
		.route("/jobs/{job_id}/logs", get(job_logs))
		.route("/repos", get(repos))
		.route("/app", get(app).put(put_app).delete(delete_app))
		.route("/app/manifest", post(manifest))
		.route("/app/conversions", post(conversion))
		.route("/accounts/sync", post(sync_accounts))
		.route("/accounts/{installation_id}/link", post(link).delete(unlink))
		.route("/accounts/{installation_id}/public", put(set_public))
		.route("/caches", get(caches))
		.route("/caches/{owner}/{name}", get(cache_repo).delete(delete_caches))
}

/// Cluster-listener-only routes (the relay reads the webhook secret).
pub fn secret_routes() -> Router<AppState> {
	Router::new().route("/app/webhook-secret", get(webhook_secret))
}

// ------------------------------------------------------------------ DTOs

fn rfc3339(at: DateTime<Utc>) -> String {
	at.to_rfc3339_opts(SecondsFormat::Millis, true)
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct App {
	pub app_id:      u64,
	pub slug:        String,
	pub name:        String,
	pub owner:       String,
	pub html_url:    String,
	pub install_url: String,
	pub webhook_url: Option<String>,
	pub created_at:  String,
}

impl From<&CiAppRow> for App {
	fn from(row: &CiAppRow) -> Self {
		Self {
			app_id:      row.app_id,
			slug:        row.slug.clone(),
			name:        row.name.clone(),
			owner:       row.owner.clone(),
			html_url:    row.html_url.clone(),
			install_url: format!("{}/installations/new", row.html_url.trim_end_matches('/')),
			webhook_url: row.webhook_url.clone(),
			created_at:  rfc3339(row.created_at),
		}
	}
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TeamRef {
	pub id:   String,
	pub slug: String,
	pub name: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Public {
	pub enabled: bool,
	pub anyone:  bool,
	pub users:   Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Account {
	pub installation_id: u64,
	pub login:           String,
	#[serde(rename = "type")]
	pub kind:            String,
	pub team:            Option<TeamRef>,
	pub suspended:       bool,
	pub public:          Public,
	pub created_at:      String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Repo {
	pub account: String,
	pub repo:    String,
	pub private: bool,
	pub allowed: bool,
	pub reason:  Option<String>,
}

/// One history record of a job (`GET /jobs/{id}` only).
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobEvent {
	pub at:         String,
	pub state:      String,
	pub message:    Option<String>,
	pub runner:     Option<String>,
	pub sandbox_id: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Job {
	pub job_id:       u64,
	pub run_id:       u64,
	pub run_attempt:  u32,
	pub account:      String,
	pub repo:         String,
	pub private:      bool,
	pub workflow:     String,
	pub job:          String,
	pub branch:       Option<String>,
	pub sha:          String,
	pub html_url:     String,
	pub labels:       Vec<String>,
	pub size:         Option<String>,
	pub from:         Option<String>,
	pub debug:        bool,
	pub state:        String,
	pub conclusion:   Option<String>,
	pub reason:       Option<String>,
	pub sandbox_id:   Option<String>,
	pub node_id:      Option<String>,
	pub runner_name:  Option<String>,
	pub queued_at:    String,
	pub started_at:   Option<String>,
	pub completed_at: Option<String>,
	pub held_until:   Option<String>,
	pub wait_ms:      Option<i64>,
	pub run_ms:       Option<i64>,
	/// History, oldest first (single-job reads only).
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub events:       Option<Vec<JobEvent>>,
}

fn wait_ms(row: &CiJobRow) -> Option<i64> {
	row.started_at
		.map(|started| (started - row.queued_at).num_milliseconds().max(0))
}

fn run_ms(row: &CiJobRow) -> Option<i64> {
	match (row.started_at, row.completed_at) {
		(Some(started), Some(completed)) => Some((completed - started).num_milliseconds().max(0)),
		_ => None,
	}
}

impl From<&CiJobRow> for Job {
	fn from(row: &CiJobRow) -> Self {
		Self {
			job_id:       row.job_id,
			run_id:       row.run_id,
			run_attempt:  row.run_attempt,
			account:      row.account.clone(),
			repo:         row.repo.clone(),
			private:      row.private,
			workflow:     row.workflow.clone(),
			job:          row.job.clone(),
			branch:       row.branch.clone(),
			sha:          row.sha.clone(),
			html_url:     row.html_url.clone(),
			labels:       row.labels.clone(),
			size:         row.spec.as_ref().map(|spec| spec.size.label().to_owned()),
			from:         row.spec.as_ref().and_then(|spec| spec.template.clone()),
			debug:        row.spec.as_ref().is_some_and(|spec| spec.debug),
			state:        row.state.as_str().to_owned(),
			conclusion:   row.conclusion.clone(),
			reason:       row.reason.clone(),
			sandbox_id:   row.sandbox_id.clone(),
			node_id:      row.node_id.clone(),
			runner_name:  row.runner_name.clone().or_else(|| row.runner.clone()),
			queued_at:    rfc3339(row.queued_at),
			started_at:   row.started_at.map(rfc3339),
			completed_at: row.completed_at.map(rfc3339),
			held_until:   row.held_until.map(rfc3339),
			wait_ms:      wait_ms(row),
			run_ms:       run_ms(row),
			events:       None,
		}
	}
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Stats {
	pub since:       String,
	pub jobs:        u64,
	pub succeeded:   u64,
	pub failed:      u64,
	pub cancelled:   u64,
	pub refused:     u64,
	pub wait_p50_ms: Option<i64>,
	pub wait_p95_ms: Option<i64>,
	pub run_seconds: i64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Overview {
	pub app:           Option<App>,
	pub dashboard_url: Option<String>,
	pub accounts:      Vec<Account>,
	pub problems:      Vec<String>,
	pub stats:         Stats,
	pub jobs:          Vec<Job>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JobPage {
	pub jobs:       Vec<Job>,
	pub next_token: Option<String>,
}

/// One repository's CI cache.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheRepo {
	pub repo_id:      u64,
	/// `owner/name` of the newest entry.
	pub repo:         String,
	/// Ready and uploading entries.
	pub entries:      usize,
	pub size_bytes:   u64,
	/// Size the sweep evicts down to (least recently used first).
	pub budget_bytes: u64,
	/// Last restore or upload of a ready entry.
	pub last_used_at: Option<String>,
}

/// One cache entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CacheEntry {
	pub id:           u64,
	pub key:          String,
	pub version:      String,
	/// Ref the entry belongs to (`refs/heads/main`, `refs/pull/5/merge`, …).
	pub scope:        String,
	/// `uploading` or `ready`.
	pub state:        String,
	pub size_bytes:   u64,
	pub digest:       Option<String>,
	/// Nodes holding the archive.
	pub holders:      Vec<String>,
	/// Job that created the entry.
	pub job_id:       u64,
	pub created_at:   String,
	pub last_used_at: String,
}

/// `GET /caches`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheRepos {
	pub repos: Vec<CacheRepo>,
}

/// `GET /caches/{owner}/{name}`.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheRepoEntries {
	pub repo:    CacheRepo,
	/// Most recently used first.
	pub entries: Vec<CacheEntry>,
}

/// `DELETE /caches/{owner}/{name}`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CachesDeleted {
	pub deleted:     usize,
	pub freed_bytes: u64,
}

/// Per-repository totals of cache `entries`, by repository name.
pub fn cache_repos(entries: &[CacheEntryRow], budget: u64) -> Vec<CacheRepo> {
	struct Totals<'a> {
		name:      &'a str,
		named_at:  DateTime<Utc>,
		entries:   usize,
		size:      u64,
		last_used: Option<DateTime<Utc>>,
	}
	let mut repos: BTreeMap<u64, Totals<'_>> = BTreeMap::new();
	for row in entries {
		let totals = repos.entry(row.repo_id).or_insert(Totals {
			name:      &row.repo,
			named_at:  row.created_at,
			entries:   0,
			size:      0,
			last_used: None,
		});
		// A renamed repository shows its newest name.
		if row.created_at > totals.named_at {
			totals.name = &row.repo;
			totals.named_at = row.created_at;
		}
		totals.entries += 1;
		totals.size += row.size;
		if row.state == EntryState::Ready {
			totals.last_used = totals.last_used.max(Some(row.last_used_at));
		}
	}
	let mut repos: Vec<CacheRepo> = repos
		.into_iter()
		.map(|(repo_id, totals)| CacheRepo {
			repo_id,
			repo: totals.name.to_owned(),
			entries: totals.entries,
			size_bytes: totals.size,
			budget_bytes: budget,
			last_used_at: totals.last_used.map(rfc3339),
		})
		.collect();
	repos.sort_by_cached_key(|repo| (repo.repo.to_lowercase(), repo.repo_id));
	repos
}

/// `row` with the nodes holding its archive (`blobs` by digest).
pub fn cache_entry(row: &CacheEntryRow, blobs: &BTreeMap<String, CacheBlobRow>) -> CacheEntry {
	let holders: Vec<String> = row
		.digest
		.as_ref()
		.and_then(|digest| blobs.get(digest))
		.map(|blob| blob.holders.iter().map(ToString::to_string).collect())
		.unwrap_or_default();
	CacheEntry {
		id: row.id,
		key: row.key.clone(),
		version: row.version.clone(),
		scope: row.scope.clone(),
		state: match row.state {
			EntryState::Uploading => "uploading",
			EntryState::Ready => "ready",
		}
		.to_owned(),
		size_bytes: row.size,
		digest: row.digest.clone(),
		holders,
		job_id: row.job_id,
		created_at: rfc3339(row.created_at),
		last_used_at: rfc3339(row.last_used_at),
	}
}

/// Id of repository `name` (`owner/name`, case-insensitive) among the
/// entries' repositories; the most recently used one when a deleted and
/// recreated repository left entries under two ids.
pub fn cache_repo_id(entries: &[CacheEntryRow], name: &str) -> Option<u64> {
	entries
		.iter()
		.filter(|row| row.repo.eq_ignore_ascii_case(name))
		.max_by_key(|row| row.last_used_at)
		.map(|row| row.repo_id)
}

// --------------------------------------------------------------- helpers

fn invalid(message: impl Into<String>) -> ApiError {
	ApiError::bad_request(message).with_code("invalid")
}

async fn team_ref(state: &AppState, team_id: Option<&str>) -> ApiResult<Option<TeamRef>> {
	let Some(id) = team_id else {
		return Ok(None);
	};
	Ok(state.store.team_by_id(id).await?.map(|team| TeamRef {
		id:   team.id,
		slug: team.slug,
		name: team.name,
	}))
}

async fn account_dto(state: &AppState, row: &CiAccountRow) -> ApiResult<Account> {
	Ok(Account {
		installation_id: row.installation_id,
		login:           row.login.clone(),
		kind:            row.kind.clone(),
		team:            team_ref(state, row.team_id.as_deref()).await?,
		suspended:       row.suspended,
		public:          Public {
			enabled: row.public.enabled,
			anyone:  row.public.anyone,
			users:   row.public.users.clone(),
		},
		created_at:      rfc3339(row.created_at),
	})
}

async fn require_github(state: &AppState) -> ApiResult<Github> {
	Github::load(state)
		.await?
		.ok_or_else(|| ApiError::not_found("no GitHub App is configured for vmon CI"))
}

fn dashboard_url(state: &AppState, app: Option<&CiAppRow>) -> String {
	app.and_then(|app| app.dashboard_url.clone())
		.unwrap_or_else(|| format!("{}/dashboard", state.config.public_url.trim_end_matches('/')))
}

/// The `p`-th percentile (0–100) of sorted `values`.
fn percentile(sorted: &[i64], p: usize) -> Option<i64> {
	if sorted.is_empty() {
		return None;
	}
	let index = (sorted.len() * p).div_ceil(100).saturating_sub(1);
	sorted.get(index.min(sorted.len() - 1)).copied()
}

/// Statistics of the jobs queued since `since`.
pub fn stats(jobs: &[CiJobRow], since: DateTime<Utc>) -> Stats {
	let recent: Vec<&CiJobRow> = jobs.iter().filter(|job| job.queued_at >= since).collect();
	let count = |keep: &dyn Fn(&CiJobRow) -> bool| {
		u64::try_from(recent.iter().filter(|job| keep(job)).count()).unwrap_or(u64::MAX)
	};
	let mut waits: Vec<i64> = recent.iter().filter_map(|job| wait_ms(job)).collect();
	waits.sort_unstable();
	Stats {
		since:       rfc3339(since),
		jobs:        u64::try_from(recent.len()).unwrap_or(u64::MAX),
		succeeded:   count(&|job| job.conclusion.as_deref() == Some("success")),
		failed:      count(&|job| {
			job.state == JobState::Failed
				|| matches!(
					job.conclusion.as_deref(),
					Some("failure" | "timed_out" | "startup_failure")
				)
		}),
		cancelled:   count(&|job| job.conclusion.as_deref() == Some("cancelled")),
		refused:     count(&|job| job.state == JobState::Refused),
		wait_p50_ms: percentile(&waits, 50),
		wait_p95_ms: percentile(&waits, 95),
		run_seconds: recent.iter().filter_map(|job| run_ms(job)).sum::<i64>() / 1000,
	}
}

/// The permissions vmon CI needs that `granted` lacks, as GitHub names them
/// (`None`: all granted).
fn missing_permissions(granted: &std::collections::BTreeMap<String, String>) -> Option<String> {
	let level = |name: &str| granted.get(name).map(String::as_str);
	let mut missing = Vec::new();
	if level("administration") != Some("write") {
		missing.push("Administration: write");
	}
	if !matches!(level("actions"), Some("read" | "write")) {
		missing.push("Actions: read");
	}
	(!missing.is_empty()).then(|| missing.join(" and "))
}

/// Setup problems an operator has to fix.
async fn problems(
	state: &AppState,
	app: Option<&CiAppRow>,
	accounts: &[CiAccountRow],
) -> ApiResult<Vec<String>> {
	let mut problems = Vec::new();
	let Some(app) = app else {
		problems.push("no GitHub App configured (vmon admin ci connect)".to_owned());
		return Ok(problems);
	};
	if let Some(missing) = app.permissions.as_ref().and_then(missing_permissions) {
		problems.push(format!(
			"the GitHub App {} does not request {missing}; vmon CI cannot work until it does",
			app.slug
		));
	}
	if accounts.is_empty() {
		problems.push(
			"the GitHub App is not installed on any account (install it, then vmon admin ci sync)"
				.to_owned(),
		);
	}
	let largest = Size::XLarge;
	let needed_s = i64::try_from(RUNTIME_LIMIT.as_secs()).unwrap_or(i64::MAX) + 600;
	for account in accounts {
		if account.suspended {
			problems.push(format!("the GitHub App is suspended on {}", account.login));
		}
		if let Some(missing) = account.permissions.as_ref().and_then(missing_permissions) {
			problems.push(format!(
				"installation {} lacks {missing}; {}",
				account.login,
				if missing.starts_with("Administration") {
					"runners cannot be registered"
				} else {
					"job status and logs cannot be read"
				}
			));
		}
		let Some(team_id) = &account.team_id else {
			problems.push(format!(
				"{} is not linked to a team: its jobs are refused (vmon admin ci link {} --team \
				 <team>)",
				account.login, account.login
			));
			continue;
		};
		let Some(team) = state.store.team_by_id(team_id).await? else {
			problems.push(format!("the team linked to {} no longer exists", account.login));
			continue;
		};
		team_problems(state, &team, largest, needed_s, &mut problems).await?;
	}
	problems.dedup();
	Ok(problems)
}

async fn team_problems(
	state: &AppState,
	team: &Team,
	largest: Size,
	needed_s: i64,
	problems: &mut Vec<String>,
) -> ApiResult<()> {
	let limits = team.limits;
	if team.blocked {
		problems.push(format!("team {} is blocked", team.slug));
	}
	if limits.max_vcpus < largest.vcpus() || limits.max_memory_mb < largest.memory_mb() {
		let fits: Vec<&str> = Size::ALL
			.iter()
			.filter(|size| {
				size.vcpus() <= limits.max_vcpus && size.memory_mb() <= limits.max_memory_mb
			})
			.map(|size| size.label())
			.collect();
		problems.push(format!(
			"team {} allows {} vCPUs and {} MiB per sandbox: only {} jobs can run",
			team.slug,
			limits.max_vcpus,
			limits.max_memory_mb,
			if fits.is_empty() {
				"no".to_owned()
			} else {
				fits.join(", ")
			}
		));
	}
	if i64::from(limits.max_length_hours) * 3600 < needed_s {
		problems.push(format!(
			"team {} caps sandboxes at {} h: jobs longer than that are killed",
			team.slug, limits.max_length_hours
		));
	}
	let template = &state.config.ci_template;
	let found = match crate::builds::names::parse_name(template) {
		Ok(name) => crate::builds::find_template(state, team, &name)
			.await?
			.is_some(),
		Err(_) => false,
	};
	if !found {
		problems.push(format!(
			"team {} cannot resolve the CI template '{template}' (build templates/ci)",
			team.slug
		));
	}
	Ok(())
}

// -------------------------------------------------------------- overview

async fn overview(State(state): State<AppState>) -> ApiResult<Json<Overview>> {
	let app = state.store.ci_app().await?;
	let accounts = state.store.ci_accounts().await?;
	let since = store::now() - Duration::days(7);
	let rows = state.store.ci_jobs(None, usize::MAX, |_| true).await?;
	let mut account_dtos = Vec::with_capacity(accounts.len());
	for account in &accounts {
		account_dtos.push(account_dto(&state, account).await?);
	}
	Ok(Json(Overview {
		app:           app.as_ref().map(App::from),
		dashboard_url: Some(dashboard_url(&state, app.as_ref())),
		accounts:      account_dtos,
		problems:      problems(&state, app.as_ref(), &accounts).await?,
		stats:         stats(&rows, since),
		jobs:          rows.iter().take(20).map(Job::from).collect(),
	}))
}

// ------------------------------------------------------------------ jobs

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JobsQuery {
	account:    Option<String>,
	repo:       Option<String>,
	state:      Option<String>,
	limit:      Option<usize>,
	next_token: Option<String>,
}

async fn jobs(
	State(state): State<AppState>,
	Query(query): Query<JobsQuery>,
) -> ApiResult<Response> {
	let limit = query.limit.unwrap_or(50).clamp(1, 500);
	let after = match query
		.next_token
		.as_deref()
		.filter(|token| !token.is_empty())
	{
		Some(token) => Some(
			token
				.parse::<u64>()
				.map_err(|_| invalid("malformed nextToken"))?,
		),
		None => None,
	};
	let filter = query.state.as_deref().unwrap_or("all").to_owned();
	if !matches!(filter.as_str(), "all" | "active" | "failed" | "refused") {
		return Err(invalid("state must be all, active, failed, or refused"));
	}
	let account = query.account.filter(|account| !account.is_empty());
	let repo = query.repo.filter(|repo| !repo.is_empty());
	let keep = |job: &CiJobRow| {
		account
			.as_ref()
			.is_none_or(|account| job.account.eq_ignore_ascii_case(account))
			&& repo.as_ref().is_none_or(|repo| {
				job.repo.eq_ignore_ascii_case(repo)
					|| job
						.repo
						.split_once('/')
						.is_some_and(|(_, name)| name.eq_ignore_ascii_case(repo))
			}) && match filter.as_str() {
			"active" => !job.state.is_finished(),
			"failed" => {
				job.state == JobState::Failed
					|| matches!(
						job.conclusion.as_deref(),
						Some("failure" | "timed_out" | "startup_failure")
					)
			},
			"refused" => job.state == JobState::Refused,
			_ => true,
		}
	};
	let mut rows = state.store.ci_jobs(after, limit + 1, keep).await?;
	let next = (rows.len() > limit).then(|| {
		rows.truncate(limit);
		rows.last().map(|job| job.job_id.to_string())
	});
	let next = next.flatten();
	let page =
		JobPage { jobs: rows.iter().map(Job::from).collect(), next_token: next.clone() };
	let mut response = Json(page).into_response();
	if let Some(next) = next
		&& let Ok(value) = next.parse()
	{
		response
			.headers_mut()
			.insert(crate::api::pagination::NEXT_TOKEN_HEADER, value);
	}
	Ok(response)
}

async fn job_row(state: &AppState, job_id: u64) -> ApiResult<CiJobRow> {
	state
		.store
		.ci_job(job_id)
		.await?
		.ok_or_else(|| ApiError::not_found(format!("CI job {job_id} not found")))
}

async fn job(State(state): State<AppState>, Path(job_id): Path<u64>) -> ApiResult<Json<Job>> {
	let row = job_row(&state, job_id).await?;
	let mut job = Job::from(&row);
	job.events = Some(
		state
			.store
			.ci_job_events(job_id)
			.await?
			.into_iter()
			.map(|logged| JobEvent {
				at:         rfc3339(logged.at),
				state:      logged.value.state.as_str().to_owned(),
				message:    logged.value.message,
				runner:     logged.value.runner,
				sandbox_id: logged.value.sandbox_id,
			})
			.collect(),
	);
	Ok(Json(job))
}

async fn job_logs(State(state): State<AppState>, Path(job_id): Path<u64>) -> ApiResult<Response> {
	let row = job_row(&state, job_id).await?;
	let done = row.state.is_finished() || row.state == JobState::Held;
	if !done || row.completed_at.is_none() {
		return Err(
			ApiError::conflict("the job log is available once the job completes")
				.with_code("not_available"),
		);
	}
	let github = require_github(&state).await?;
	let log = github
		.job_logs(row.installation_id, &row.repo, job_id)
		.await
		.map_err(|error| {
			if error.is(StatusCode::NOT_FOUND) || error.is(StatusCode::GONE) {
				ApiError::not_found(format!("GitHub has no log for job {job_id}"))
			} else {
				error.into()
			}
		})?;
	Ok(([(header::CONTENT_TYPE, "text/plain; charset=utf-8")], log).into_response())
}

// ----------------------------------------------------------------- repos

#[derive(Debug, Default, Deserialize)]
struct ReposQuery {
	account: Option<String>,
}

/// Whether jobs of `repo` may run, and why not.
fn repo_verdict(account: &CiAccountRow, private: bool) -> (bool, Option<String>) {
	if account.suspended {
		return (false, Some("the GitHub App is suspended on this account".into()));
	}
	if account.team_id.is_none() {
		return (false, Some("the account is not linked to a vmon team".into()));
	}
	if private {
		return (true, None);
	}
	let policy = &account.public;
	if !policy.enabled {
		(false, Some("public repositories are off for this account".into()))
	} else if policy.anyone {
		(true, None)
	} else if policy.users.is_empty() {
		(false, Some("public repositories: no user is allowed".into()))
	} else {
		(true, Some(format!("only runs by {}", policy.users.join(", "))))
	}
}

async fn repos(
	State(state): State<AppState>,
	Query(query): Query<ReposQuery>,
) -> ApiResult<Json<serde_json::Value>> {
	let github = require_github(&state).await?;
	let filter = query.account.filter(|account| !account.is_empty());
	let mut repos = Vec::new();
	for account in state.store.ci_accounts().await? {
		if filter
			.as_ref()
			.is_some_and(|login| !account.login.eq_ignore_ascii_case(login))
		{
			continue;
		}
		for repo in github.installation_repos(account.installation_id).await? {
			let (allowed, reason) = repo_verdict(&account, repo.private);
			repos.push(Repo {
				account: account.login.clone(),
				repo: repo.full_name,
				private: repo.private,
				allowed,
				reason,
			});
		}
	}
	Ok(Json(json!({ "repos": repos })))
}

// ---------------------------------------------------------------- caches

fn cache_error(error: &anyhow::Error) -> ApiError {
	ApiError::unavailable(format!("CI cache index unavailable: {error:#}"))
}

async fn caches(State(state): State<AppState>) -> ApiResult<Json<CacheRepos>> {
	let entries = index::entries(&state.cluster.kv, None)
		.await
		.map_err(|error| cache_error(&error))?;
	Ok(Json(CacheRepos { repos: cache_repos(&entries, index::REPO_BUDGET) }))
}

/// The cache of repository `owner/name` and its entries (404 without
/// entries).
async fn repo_cache(
	state: &AppState,
	owner: &str,
	name: &str,
) -> ApiResult<(CacheRepo, Vec<CacheEntryRow>)> {
	let full = format!("{owner}/{name}");
	let not_found =
		|| ApiError::not_found(format!("no CI cache entries for {full}")).with_code("not_found");
	let entries = index::entries(&state.cluster.kv, None)
		.await
		.map_err(|error| cache_error(&error))?;
	let repo_id = cache_repo_id(&entries, &full).ok_or_else(not_found)?;
	let entries: Vec<CacheEntryRow> = entries
		.into_iter()
		.filter(|row| row.repo_id == repo_id)
		.collect();
	let repo = cache_repos(&entries, index::REPO_BUDGET)
		.into_iter()
		.next()
		.ok_or_else(not_found)?;
	Ok((repo, entries))
}

async fn cache_repo(
	State(state): State<AppState>,
	Path((owner, name)): Path<(String, String)>,
) -> ApiResult<Json<CacheRepoEntries>> {
	let (repo, mut rows) = repo_cache(&state, &owner, &name).await?;
	let blobs = index::blobs(&state.cluster.kv)
		.await
		.map_err(|error| cache_error(&error))?;
	rows.sort_by_key(|row| std::cmp::Reverse(row.last_used_at));
	let entries = rows.iter().map(|row| cache_entry(row, &blobs)).collect();
	Ok(Json(CacheRepoEntries { repo, entries }))
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct DeleteCachesQuery {
	#[serde(default)]
	key_prefix: String,
}

/// `DELETE /caches/{owner}/{name}?keyPrefix=`: the repository's entries
/// (ready and uploading) whose key starts with the prefix.
async fn delete_caches(
	State(state): State<AppState>,
	Path((owner, name)): Path<(String, String)>,
	Query(query): Query<DeleteCachesQuery>,
) -> ApiResult<Json<CachesDeleted>> {
	let (_, rows) = repo_cache(&state, &owner, &name).await?;
	let mut deleted = CachesDeleted::default();
	for row in rows
		.iter()
		.filter(|row| row.key.starts_with(&query.key_prefix))
	{
		let key = records::entry_key(row.repo_id, &row.scope, &row.key, &row.version);
		if let Some(gone) = index::delete_entry(&state.cluster.kv, &key, Some(row.id))
			.await
			.map_err(|error| cache_error(&error))?
		{
			deleted.deleted += 1;
			deleted.freed_bytes += gone.size;
		}
	}
	Ok(Json(deleted))
}

// ------------------------------------------------------------------- app

async fn app(State(state): State<AppState>) -> ApiResult<Json<App>> {
	let app = state
		.store
		.ci_app()
		.await?
		.ok_or_else(|| ApiError::not_found("no GitHub App is configured for vmon CI"))?;
	Ok(Json(App::from(&app)))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct PutApp {
	app_id:         u64,
	private_key:    String,
	webhook_secret: String,
}

/// `PUT /app`: credentials checked against GitHub (`GET /app`).
async fn put_app(State(state): State<AppState>, Json(body): Json<PutApp>) -> ApiResult<Json<App>> {
	if body.webhook_secret.trim().is_empty() {
		return Err(invalid("webhookSecret must not be empty"));
	}
	let pem = body.private_key.trim().as_bytes().to_vec();
	let github =
		Github::with_key(&state, body.app_id, &pem).map_err(|error| invalid(error.message))?;
	let info = github.app().await.map_err(|error| {
		if error.is(StatusCode::UNAUTHORIZED) {
			invalid(format!("GitHub rejected the app id or private key: {}", error.message))
		} else {
			error.into()
		}
	})?;
	if info.id != body.app_id {
		return Err(invalid(format!(
			"the private key belongs to app {}, not {}",
			info.id, body.app_id
		)));
	}
	let webhook_url = github.webhook_url().await.ok().flatten();
	let previous = state.store.ci_app().await?;
	let now = store::now();
	let row = CiAppRow {
		app_id: info.id,
		slug: info.slug,
		name: info.name,
		owner: info.owner.login,
		html_url: info.html_url,
		webhook_url,
		dashboard_url: previous.as_ref().and_then(|app| app.dashboard_url.clone()),
		client_id: previous.as_ref().and_then(|app| app.client_id.clone()),
		client_secret_enc: previous
			.as_ref()
			.and_then(|app| app.client_secret_enc.clone()),
		private_key_enc: state.keys.encrypt(&pem),
		webhook_secret_enc: state.keys.encrypt(body.webhook_secret.trim().as_bytes()),
		permissions: Some(info.permissions),
		created_at: previous.as_ref().map_or(now, |app| app.created_at),
		updated_at: now,
	};
	state.store.put_ci_app(&row).await?;
	state.ci.forget_tokens();
	Ok(Json(App::from(&row)))
}

async fn delete_app(State(state): State<AppState>) -> ApiResult<StatusCode> {
	state.store.delete_ci_app().await?;
	state.ci.forget_tokens();
	Ok(StatusCode::NO_CONTENT)
}

async fn webhook_secret(State(state): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
	let app = state
		.store
		.ci_app()
		.await?
		.ok_or_else(|| ApiError::not_found("no GitHub App is configured for vmon CI"))?;
	let secret = state
		.keys
		.decrypt_str(&app.webhook_secret_enc)
		.map_err(|error| ApiError::internal(format!("the webhook secret is unreadable: {error}")))?;
	Ok(Json(json!({ "webhookSecret": secret })))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestRequest {
	dashboard_url: String,
	webhook_url:   String,
	#[serde(default)]
	org:           Option<String>,
	#[serde(default)]
	public:        bool,
	#[serde(default)]
	name:          Option<String>,
}

fn http_url(field: &str, raw: &str) -> ApiResult<String> {
	let url = url::Url::parse(raw.trim()).map_err(|error| invalid(format!("{field}: {error}")))?;
	if !matches!(url.scheme(), "http" | "https") {
		return Err(invalid(format!("{field} must be an http(s) URL")));
	}
	Ok(url.as_str().trim_end_matches('/').to_owned())
}

/// `POST /app/manifest`: starts GitHub's app manifest flow.
async fn manifest(
	State(state): State<AppState>,
	Json(body): Json<ManifestRequest>,
) -> ApiResult<Json<serde_json::Value>> {
	let dashboard = http_url("dashboardUrl", &body.dashboard_url)?;
	let webhook = http_url("webhookUrl", &body.webhook_url)?;
	let flow = hex::encode(rand::random::<[u8; 16]>());
	state
		.store
		.put_ci_manifest(&flow, &CiManifestRow {
			dashboard_url: dashboard.clone(),
			webhook_url:   webhook.clone(),
			created_at:    store::now(),
		})
		.await?;
	let name = body
		.name
		.filter(|name| !name.trim().is_empty())
		.unwrap_or_else(|| format!("vmon CI ({})", state.config.domain));
	let manifest = json!({
		"name": name,
		"url": dashboard,
		"hook_attributes": { "url": webhook, "active": true },
		"redirect_url": format!("{dashboard}/ci/connect"),
		"setup_url": format!("{dashboard}/ci/install"),
		"setup_on_update": true,
		"public": body.public,
		"default_permissions": {
			"actions": "read",
			"administration": "write",
			"metadata": "read",
		},
		"default_events": ["workflow_job"],
	});
	let web = github::web_url(&state.config.ci_github_api);
	let action = match body
		.org
		.as_deref()
		.map(str::trim)
		.filter(|org| !org.is_empty())
	{
		Some(org) => format!("{web}/organizations/{org}/settings/apps/new?state={flow}"),
		None => format!("{web}/settings/apps/new?state={flow}"),
	};
	Ok(Json(json!({ "action": action, "manifest": manifest.to_string() })))
}

#[derive(Debug, Deserialize)]
struct ConversionRequest {
	code:  String,
	state: String,
}

/// `POST /app/conversions`: finishes the manifest flow.
async fn conversion(
	State(state): State<AppState>,
	Json(body): Json<ConversionRequest>,
) -> ApiResult<Json<App>> {
	let Some(flow) = state.store.take_ci_manifest(&body.state).await? else {
		return Err(invalid("unknown or expired manifest flow; start again"));
	};
	if body.code.is_empty() || !body.code.bytes().all(|b| b.is_ascii_alphanumeric()) {
		return Err(invalid("malformed code"));
	}
	let created = github::convert_manifest(&state, &body.code).await?;
	let now = store::now();
	let row = CiAppRow {
		app_id:             created.id,
		slug:               created.slug,
		name:               created.name,
		owner:              created.owner.login,
		html_url:           created.html_url,
		webhook_url:        Some(flow.webhook_url),
		dashboard_url:      Some(flow.dashboard_url),
		client_id:          created.client_id,
		client_secret_enc:  created
			.client_secret
			.map(|secret| state.keys.encrypt(secret.as_bytes())),
		private_key_enc:    state.keys.encrypt(created.pem.as_bytes()),
		webhook_secret_enc: state.keys.encrypt(created.webhook_secret.as_bytes()),
		permissions:        Some(created.permissions),
		created_at:         now,
		updated_at:         now,
	};
	state.store.put_ci_app(&row).await?;
	state.ci.forget_tokens();
	Ok(Json(App::from(&row)))
}

// -------------------------------------------------------------- accounts

fn seen(installation: github::Installation) -> InstallationSeen {
	InstallationSeen {
		installation_id: installation.id,
		login:           installation.account.login,
		kind:            installation.account.kind,
		suspended:       installation.suspended_at.is_some(),
		permissions:     Some(installation.permissions),
	}
}

async fn sync_accounts(State(state): State<AppState>) -> ApiResult<Json<serde_json::Value>> {
	let github = require_github(&state).await?;
	// An owner can grant a permission after the app was configured: refresh
	// the app's own permissions, which the setup problems read, as well.
	let info = github.app().await?;
	if let Some(mut app) = state.store.ci_app().await?
		&& app.app_id == info.id
		&& app.permissions.as_ref() != Some(&info.permissions)
	{
		app.permissions = Some(info.permissions);
		app.updated_at = store::now();
		state.store.put_ci_app(&app).await?;
	}
	let installations: Vec<InstallationSeen> = github
		.installations()
		.await?
		.into_iter()
		.map(seen)
		.collect();
	let rows = state.store.sync_ci_accounts(&installations).await?;
	let mut accounts = Vec::with_capacity(rows.len());
	for row in &rows {
		accounts.push(account_dto(&state, row).await?);
	}
	state.ci.wake();
	Ok(Json(json!({ "accounts": accounts })))
}

/// The account of an installation, fetched from GitHub when not recorded
/// yet (the dashboard links right after GitHub's install redirect).
async fn known_account(state: &AppState, installation_id: u64) -> ApiResult<CiAccountRow> {
	if let Some(row) = state.store.ci_account(installation_id).await? {
		return Ok(row);
	}
	let github = require_github(state).await?;
	let installation = github
		.installation(installation_id)
		.await
		.map_err(|error| {
			if error.is(StatusCode::NOT_FOUND) {
				ApiError::not_found(format!(
					"installation {installation_id} of the GitHub App not found"
				))
			} else {
				error.into()
			}
		})?;
	Ok(state.store.upsert_ci_account(&seen(installation)).await?)
}

async fn update_account(
	state: &AppState,
	installation_id: u64,
	change: impl Fn(&mut CiAccountRow),
) -> ApiResult<Json<Account>> {
	known_account(state, installation_id).await?;
	let row = state
		.store
		.update_ci_account(installation_id, change)
		.await?
		.ok_or_else(|| ApiError::not_found(format!("installation {installation_id} not found")))?;
	state.ci.wake();
	Ok(Json(account_dto(state, &row).await?))
}

#[derive(Debug, Deserialize)]
struct LinkRequest {
	team: String,
}

async fn link(
	State(state): State<AppState>,
	Path(installation_id): Path<u64>,
	Json(body): Json<LinkRequest>,
) -> ApiResult<Json<Account>> {
	let team = state
		.store
		.team_by_ref(body.team.trim())
		.await?
		.ok_or_else(|| ApiError::not_found(format!("team {} not found", body.team)))?;
	update_account(&state, installation_id, |row| row.team_id = Some(team.id.clone())).await
}

async fn unlink(
	State(state): State<AppState>,
	Path(installation_id): Path<u64>,
) -> ApiResult<Json<Account>> {
	update_account(&state, installation_id, |row| row.team_id = None).await
}

#[derive(Debug, Deserialize)]
struct PublicRequest {
	enabled: bool,
	#[serde(default)]
	anyone:  bool,
	#[serde(default)]
	users:   Vec<String>,
}

/// GitHub logins of `raw` (`@` and blanks dropped, duplicates removed).
pub fn normalize_users(raw: &[String]) -> Vec<String> {
	let mut users: Vec<String> = Vec::new();
	for entry in raw
		.iter()
		.flat_map(|entry| entry.split([',', ' ', '\n', '\t']))
	{
		let login = entry.trim().trim_start_matches('@');
		if !login.is_empty() && !users.iter().any(|user| user.eq_ignore_ascii_case(login)) {
			users.push(login.to_owned());
		}
	}
	users
}

async fn set_public(
	State(state): State<AppState>,
	Path(installation_id): Path<u64>,
	Json(body): Json<PublicRequest>,
) -> ApiResult<Json<Account>> {
	let users = normalize_users(&body.users);
	let account = known_account(&state, installation_id).await?;
	if !users.is_empty() {
		let github = require_github(&state).await?;
		let mut unknown = Vec::new();
		for user in &users {
			if !github.user_exists(account.installation_id, user).await? {
				unknown.push(user.clone());
			}
		}
		if !unknown.is_empty() {
			return Err(invalid(format!("unknown GitHub users: {}", unknown.join(", "))));
		}
	}
	let policy = PublicPolicy { enabled: body.enabled, anyone: body.anyone, users };
	update_account(&state, installation_id, |row| row.public = policy.clone()).await
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn users_are_normalized() {
		assert_eq!(normalize_users(&["a, @b".into(), " A ".into(), "@c\nd".into()]), [
			"a", "b", "c", "d"
		]);
	}

	fn entry(repo_id: u64, repo: &str, key: &str, size: u64, age_h: i64) -> CacheEntryRow {
		let at = store::now() - Duration::hours(age_h);
		CacheEntryRow {
			id: repo_id * 100 + size,
			repo_id,
			repo: repo.into(),
			scope: "refs/heads/main".into(),
			key: key.into(),
			version: "v1".into(),
			state: EntryState::Ready,
			digest: Some(format!("{size:064x}")),
			size,
			node: vmon_cluster::NodeId(7),
			job_id: 9,
			created_at: at,
			last_used_at: at,
		}
	}

	#[test]
	fn cache_repos_total_their_entries() {
		let mut uploading = entry(2, "acme/lib", "npm-", 0, 0);
		uploading.state = EntryState::Uploading;
		uploading.digest = None;
		let entries = [
			entry(1, "acme/old-name", "cargo-a", 10, 5),
			entry(1, "acme/app", "cargo-b", 20, 1),
			entry(2, "Acme/lib", "npm-a", 5, 3),
			uploading,
		];
		let repos = cache_repos(&entries, 1000);
		assert_eq!(repos.len(), 2);
		assert_eq!(repos[0], CacheRepo {
			repo_id:      1,
			repo:         "acme/app".into(),
			entries:      2,
			size_bytes:   30,
			budget_bytes: 1000,
			last_used_at: Some(rfc3339(entries[1].last_used_at)),
		});
		assert_eq!((repos[1].repo.as_str(), repos[1].entries), ("acme/lib", 2));
		assert_eq!(repos[1].last_used_at, Some(rfc3339(entries[2].last_used_at)), "ready only");
		assert_eq!(cache_repo_id(&entries, "ACME/APP"), Some(1));
		assert_eq!(cache_repo_id(&entries, "acme/old-name"), Some(1));
		assert_eq!(cache_repo_id(&entries, "acme/none"), None);
	}

	#[test]
	fn cache_entries_name_their_holders() {
		let row = entry(1, "acme/app", "cargo-a", 10, 1);
		let blobs = BTreeMap::from([(row.digest.clone().unwrap(), CacheBlobRow {
			size:       10,
			holders:    vec![vmon_cluster::NodeId(7), vmon_cluster::NodeId(0xab)],
			entries:    Vec::new(),
			created_at: row.created_at,
		})]);
		let dto = cache_entry(&row, &blobs);
		assert_eq!(dto.state, "ready");
		assert_eq!(dto.holders, ["0000000000000007", "00000000000000ab"]);
		assert_eq!((dto.size_bytes, dto.job_id, dto.id), (10, 9, 110));
		let json = serde_json::to_value(&dto).unwrap();
		assert_eq!(json["sizeBytes"], 10);
		assert_eq!(json["lastUsedAt"], dto.last_used_at.as_str());
		let mut uploading = row;
		uploading.state = EntryState::Uploading;
		uploading.digest = None;
		let dto = cache_entry(&uploading, &blobs);
		assert_eq!((dto.state.as_str(), dto.holders.len()), ("uploading", 0));
	}

	#[test]
	fn percentiles() {
		assert_eq!(percentile(&[], 50), None);
		assert_eq!(percentile(&[5], 95), Some(5));
		let values: Vec<i64> = (1..=100).collect();
		assert_eq!(percentile(&values, 50), Some(50));
		assert_eq!(percentile(&values, 95), Some(95));
	}
}
