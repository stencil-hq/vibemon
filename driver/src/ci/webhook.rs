//! `POST /ci/github/webhook`: GitHub App webhooks.
//!
//! Directly from GitHub or through a relay. Verifies `X-Hub-Signature-256`
//! over the raw body with the app's webhook secret, records the event, and
//! answers at once: 202 accepted, 200 ping/ignored, 401 bad or missing
//! signature, 503 no app configured.

use std::collections::BTreeMap;

use axum::{
	Json,
	body::Bytes,
	extract::State,
	http::{HeaderMap, StatusCode},
	response::{IntoResponse, Response},
};
use hmac::{Hmac, Mac};
use serde::Deserialize;
use serde_json::json;
use sha2::Sha256;

use super::{
	github::{Account, Repository, WorkflowJob},
	labels::{self, Labels},
};
use crate::{
	error::ApiError,
	state::AppState,
	store::{
		self,
		ci::{CiDelivery, CiJobRow, GithubStatus, InstallationSeen, JobState, JobUpdate, Recorded},
	},
};

/// Whether `header` (`sha256=<hex>`) is the HMAC-SHA256 of `body` under
/// `secret` (constant-time comparison).
pub fn verify_signature(secret: &[u8], body: &[u8], header: Option<&str>) -> bool {
	let Some(signature) = header
		.and_then(|value| value.trim().strip_prefix("sha256="))
		.and_then(|hex| hex::decode(hex).ok())
	else {
		return false;
	};
	let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC accepts any key");
	mac.update(body);
	mac.verify_slice(&signature).is_ok()
}

/// `X-Hub-Signature-256` value of `body` (tests, fake GitHub servers).
pub fn sign(secret: &[u8], body: &[u8]) -> String {
	let mut mac = <Hmac<Sha256> as Mac>::new_from_slice(secret).expect("HMAC accepts any key");
	mac.update(body);
	format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
}

#[derive(Deserialize)]
struct InstallationRef {
	id: u64,
}

#[derive(Deserialize)]
struct JobPayload {
	action:       String,
	workflow_job: WorkflowJob,
	repository:   Repository,
	installation: Option<InstallationRef>,
}

#[derive(Deserialize)]
struct InstallationPayload {
	action:       String,
	installation: InstallationBody,
}

#[derive(Deserialize)]
struct InstallationBody {
	id:           u64,
	account:      Account,
	#[serde(default)]
	suspended_at: Option<String>,
	#[serde(default)]
	permissions:  Option<BTreeMap<String, String>>,
}

/// How a webhook was handled.
struct Outcome {
	status: StatusCode,
	what:   String,
}

impl Outcome {
	fn accepted(what: impl Into<String>) -> Self {
		Self { status: StatusCode::ACCEPTED, what: what.into() }
	}

	fn ignored(what: impl Into<String>) -> Self {
		Self { status: StatusCode::OK, what: what.into() }
	}
}

fn header<'h>(headers: &'h HeaderMap, name: &str) -> Option<&'h str> {
	headers.get(name).and_then(|value| value.to_str().ok())
}

/// The webhook handler.
pub async fn receive(State(state): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
	match receive_inner(&state, &headers, &body).await {
		Ok(outcome) => {
			let delivery = CiDelivery {
				delivery: header(&headers, "x-github-delivery")
					.unwrap_or_default()
					.to_owned(),
				event:    header(&headers, "x-github-event")
					.unwrap_or_default()
					.to_owned(),
				action:   serde_json::from_slice::<serde_json::Value>(&body)
					.ok()
					.and_then(|value| value.get("action")?.as_str().map(str::to_owned)),
				outcome:  outcome.what.clone(),
			};
			// Answered 2xx: the reconcile never asks GitHub to redeliver it,
			// whatever a relay in front of vmon answered GitHub.
			if !delivery.delivery.is_empty()
				&& let Err(error) = state
					.store
					.mark_ci_delivery_accepted(&delivery.delivery)
					.await
			{
				tracing::warn!(%error, "recording an accepted CI webhook failed");
			}
			if let Err(error) = state.store.log_ci_delivery(&delivery).await {
				tracing::warn!(%error, "recording a CI webhook delivery failed");
			}
			if outcome.status == StatusCode::ACCEPTED {
				state.ci.wake();
			}
			(outcome.status, Json(json!({ "status": outcome.what }))).into_response()
		},
		Err(error) => error.into_response(),
	}
}

async fn receive_inner(
	state: &AppState,
	headers: &HeaderMap,
	body: &[u8],
) -> Result<Outcome, ApiError> {
	let Some(app) = state.store.ci_app().await? else {
		return Err(ApiError::unavailable("no GitHub App is configured for vmon CI"));
	};
	let secret = state
		.keys
		.decrypt(&app.webhook_secret_enc)
		.map_err(|error| ApiError::internal(format!("the webhook secret is unreadable: {error}")))?;
	if !verify_signature(&secret, body, header(headers, "x-hub-signature-256")) {
		return Err(ApiError::unauthorized("missing or invalid X-Hub-Signature-256"));
	}
	let malformed = |error: serde_json::Error| {
		ApiError::bad_request(format!("malformed webhook payload: {error}"))
	};
	match header(headers, "x-github-event").unwrap_or_default() {
		"ping" => Ok(Outcome::ignored("pong")),
		"workflow_job" => workflow_job(state, serde_json::from_slice(body).map_err(malformed)?).await,
		"installation" | "installation_repositories" => {
			let payload: InstallationPayload = serde_json::from_slice(body).map_err(malformed)?;
			installation(state, payload).await
		},
		event => Ok(Outcome::ignored(format!("ignored event {event}"))),
	}
}

async fn installation(state: &AppState, payload: InstallationPayload) -> Result<Outcome, ApiError> {
	let id = payload.installation.id;
	if payload.action == "deleted" {
		state.store.delete_ci_account(id).await?;
		return Ok(Outcome::accepted(format!("installation {id} removed")));
	}
	let seen = InstallationSeen {
		installation_id: id,
		login:           payload.installation.account.login,
		kind:            payload.installation.account.kind,
		suspended:       payload.action == "suspend"
			|| (payload.action != "unsuspend" && payload.installation.suspended_at.is_some()),
		permissions:     payload.installation.permissions,
	};
	let row = state.store.upsert_ci_account(&seen).await?;
	Ok(Outcome::accepted(format!("installation {id} ({}) recorded", row.login)))
}

/// GitHub's job status string as a [`GithubStatus`] (`None`: not tracked,
/// e.g. `waiting` on a deployment approval).
pub fn github_status(status: &str) -> Option<GithubStatus> {
	match status {
		"queued" => Some(GithubStatus::Queued),
		"in_progress" => Some(GithubStatus::InProgress),
		"completed" => Some(GithubStatus::Completed),
		_ => None,
	}
}

/// The job record a first sighting of `job` creates; `None` for jobs that
/// are not vmon's.
pub fn new_job(
	installation_id: u64,
	repository: &Repository,
	job: &WorkflowJob,
) -> Option<CiJobRow> {
	let (spec, state, reason) = match labels::parse(&job.labels) {
		Labels::NotOurs => return None,
		Labels::Spec(spec) => (Some(spec), JobState::Queued, None),
		Labels::Refused(reason) => (None, JobState::Refused, Some(reason)),
	};
	let now = store::now();
	Some(CiJobRow {
		job_id: job.id,
		run_id: job.run_id,
		run_attempt: job.run_attempt.max(1),
		installation_id,
		account: repository
			.full_name
			.split('/')
			.next()
			.unwrap_or_default()
			.to_owned(),
		repo: repository.full_name.clone(),
		private: repository.private,
		workflow: job.workflow_name.clone().unwrap_or_default(),
		job: job.name.clone(),
		branch: job.head_branch.clone(),
		sha: job.head_sha.clone(),
		html_url: job.html_url.clone(),
		labels: job.labels.clone(),
		spec,
		github: GithubStatus::Queued,
		state,
		conclusion: None,
		reason,
		decided: state == JobState::Refused,
		team_id: None,
		runner: None,
		provisions: 0,
		runner_name: None,
		sandbox_id: None,
		node_id: None,
		queued_at: job.created_at.unwrap_or(now),
		started_at: None,
		completed_at: None,
		held_until: None,
		updated_at: now,
	})
}

/// [`JobUpdate`] of a job report (`row`: the record a first sighting creates).
pub fn job_update(status: GithubStatus, row: CiJobRow, job: &WorkflowJob) -> JobUpdate {
	JobUpdate {
		status,
		job: row,
		runner_name: job.runner_name.clone().filter(|name| !name.is_empty()),
		conclusion: job.conclusion.clone(),
		started_at: job.started_at,
		completed_at: job.completed_at,
	}
}

async fn workflow_job(state: &AppState, payload: JobPayload) -> Result<Outcome, ApiError> {
	let job = &payload.workflow_job;
	let Some(status) = github_status(&payload.action) else {
		return Ok(Outcome::ignored(format!("ignored workflow_job action {}", payload.action)));
	};
	let Some(installation) = payload.installation else {
		return Ok(Outcome::ignored("ignored: no installation"));
	};
	let Some(row) = new_job(installation.id, &payload.repository, job) else {
		return Ok(Outcome::ignored(format!("ignored job {}: not a vmon job", job.id)));
	};
	let recorded = state
		.store
		.record_ci_job(&job_update(status, row, job))
		.await?;
	let what = match recorded {
		Recorded::New => format!("job {} recorded ({})", job.id, payload.action),
		Recorded::Updated => format!("job {} updated ({})", job.id, payload.action),
		Recorded::Duplicate => format!("job {} unchanged ({})", job.id, payload.action),
	};
	Ok(Outcome::accepted(what))
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn signatures_verify_over_the_raw_body() {
		let body = br#"{"action":"queued"}"#;
		let good = sign(b"secret", body);
		assert!(good.starts_with("sha256="));
		assert!(verify_signature(b"secret", body, Some(&good)));
		assert!(!verify_signature(b"other", body, Some(&good)));
		assert!(!verify_signature(b"secret", br#"{"action":"queued" }"#, Some(&good)));
		assert!(!verify_signature(b"secret", body, None));
		assert!(!verify_signature(b"secret", body, Some(good.trim_start_matches("sha256="))));
		assert!(!verify_signature(b"secret", body, Some("sha256=zz")));
		assert!(!verify_signature(b"secret", body, Some("sha1=00")));
	}
}
