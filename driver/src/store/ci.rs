//! vmon CI: the GitHub App, linked GitHub accounts, workflow jobs, and the
//! runners provisioned for them (ARCHITECTURE.md §9.7).
//!
//! Keys:
//! - `db/ci_app/github` → [`CiAppRow`]: the GitHub App, its private key and
//!   secrets encrypted with [`crate::crypto::Keys`].
//! - `db/ci_manifest/<state>` → [`CiManifestRow`] (TTL 1 h): a pending app
//!   manifest flow.
//! - `db/ci_account/<installation id>` → [`CiAccountRow`]: an installation of
//!   the app, its linked team, and its public-repository policy.
//! - `db/ci_job/<job id>` → [`CiJobRow`], keyed by GitHub's job id (20 digits),
//!   the dedupe key of every webhook. Finished jobs expire [`JOB_RETENTION`]
//!   after their last change.
//! - `db/ci_job.active/<job id>` → empty: unfinished jobs.
//! - `db/ci_runner/<name>` → [`CiRunnerRow`]: one JIT runner and its sandbox,
//!   deleted when torn down.
//! - `db/ci_redelivery/<delivery guid>` → [`CiRedeliveryRow`] (TTL 1 day):
//!   redeliveries the controller requested for one webhook delivery.
//! - `db/ci_delivery/<delivery guid>` → `true` (TTL 25 h): a webhook delivery
//!   vmon answered with 2xx; never redelivered.
//!
//! History goes to the journal: `ci/job/<job id>` gets a [`CiEvent`] for every
//! state or reason change of the job, `ci/deliveries` a [`CiDelivery`] per
//! webhook received.
//!
//! Transitions run in one transaction over the job and the runners it
//! touches, so webhooks that arrive twice, late, or out of order and a
//! controller that changes leaders converge on one record per job and at most
//! one sandbox per runner record: a GitHub status never moves backwards, and
//! runner steps commit only under the claim (`attempt`) that started them.

use std::{collections::BTreeMap, time::Duration};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use vmon_cluster::kv::{Range, Scan};
use vmon_node::ci_cache::records::CacheScope;

use super::{Logged, Result, Store, Tx, key, now, prefix, seq};
use crate::ci::labels::RunnerSpec;

const APPS: &str = "db/ci_app";
const APP: &str = "github";
const MANIFESTS: &str = "db/ci_manifest";
const ACCOUNTS: &str = "db/ci_account";
const JOBS: &str = "db/ci_job";
const ACTIVE_JOBS: &str = "db/ci_job.active";
const RUNNERS: &str = "db/ci_runner";
const REDELIVERIES: &str = "db/ci_redelivery";
const ACCEPTED: &str = "db/ci_delivery";
/// Journal stream of received webhooks.
const DELIVERIES_STREAM: &str = "ci/deliveries";

/// How long a finished job stays listed (covers the 7-day statistics).
pub const JOB_RETENTION: Duration = Duration::from_hours(8 * 24);
/// Lifetime of a pending manifest flow.
pub const MANIFEST_TTL: Duration = Duration::from_hours(1);
/// Lifetime of a redelivery record.
pub const REDELIVERY_TTL: Duration = Duration::from_hours(24);
/// Lifetime of an accepted-delivery mark (beyond the 24 h redelivery
/// window).
pub const ACCEPTED_TTL: Duration = Duration::from_hours(25);
/// Debug hold of a failed job's sandbox.
pub const HOLD: Duration = Duration::from_mins(30);

/// The configured GitHub App.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CiAppRow {
	pub app_id:             u64,
	pub slug:               String,
	pub name:               String,
	pub owner:              String,
	pub html_url:           String,
	#[serde(default)]
	pub webhook_url:        Option<String>,
	/// Dashboard the manifest flow ran from.
	#[serde(default)]
	pub dashboard_url:      Option<String>,
	#[serde(default)]
	pub client_id:          Option<String>,
	#[serde(default, with = "crate::store::b64_opt")]
	pub client_secret_enc:  Option<Vec<u8>>,
	#[serde(with = "crate::store::b64")]
	pub private_key_enc:    Vec<u8>,
	#[serde(with = "crate::store::b64")]
	pub webhook_secret_enc: Vec<u8>,
	/// Permissions the app requests, as last read (`None`: unknown).
	#[serde(default)]
	pub permissions:        Option<BTreeMap<String, String>>,
	pub created_at:         DateTime<Utc>,
	pub updated_at:         DateTime<Utc>,
}

/// A manifest flow awaiting GitHub's redirect.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CiManifestRow {
	pub dashboard_url: String,
	pub webhook_url:   String,
	pub created_at:    DateTime<Utc>,
}

/// Public-repository policy of an account: a run is allowed when `enabled
/// && (anyone || (actor ∈ users && triggering_actor ∈ users))`.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct PublicPolicy {
	pub enabled: bool,
	pub anyone:  bool,
	/// GitHub logins (compared case-insensitively).
	pub users:   Vec<String>,
}

impl PublicPolicy {
	/// Whether a run by `actor`, triggered by `triggering_actor`, may run.
	pub fn allows(&self, actor: &str, triggering_actor: &str) -> bool {
		let listed = |login: &str| {
			self
				.users
				.iter()
				.any(|user| user.eq_ignore_ascii_case(login))
		};
		self.enabled && (self.anyone || (listed(actor) && listed(triggering_actor)))
	}
}

/// An installation of the app (one GitHub user or organization).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CiAccountRow {
	pub installation_id: u64,
	pub login:           String,
	/// `User` or `Organization`.
	pub kind:            String,
	/// Team whose sandboxes run the account's jobs.
	#[serde(default)]
	pub team_id:         Option<String>,
	#[serde(default)]
	pub suspended:       bool,
	#[serde(default)]
	pub public:          PublicPolicy,
	/// Permissions the installation granted, as last reported (`None`:
	/// never reported).
	#[serde(default)]
	pub permissions:     Option<BTreeMap<String, String>>,
	pub created_at:      DateTime<Utc>,
	pub updated_at:      DateTime<Utc>,
}

/// An installation as GitHub reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InstallationSeen {
	pub installation_id: u64,
	pub login:           String,
	pub kind:            String,
	pub suspended:       bool,
	/// Granted permissions (`None`: the report carried none).
	pub permissions:     Option<BTreeMap<String, String>>,
}

/// Job states (the admin API's `Job.state`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JobState {
	/// Recorded, no sandbox yet.
	Queued,
	/// Retrying capacity or rate limits (`reason` says why).
	Waiting,
	/// Sandbox up, runner registering.
	Starting,
	/// A runner picked the job.
	Running,
	/// Debug hold after a failure.
	Held,
	/// Finished on GitHub (`conclusion`).
	Completed,
	/// Never ran (`reason`).
	Refused,
	/// vmon-side failure (`reason`).
	Failed,
}

impl JobState {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Queued => "queued",
			Self::Waiting => "waiting",
			Self::Starting => "starting",
			Self::Running => "running",
			Self::Held => "held",
			Self::Completed => "completed",
			Self::Refused => "refused",
			Self::Failed => "failed",
		}
	}

	/// No further work: the record only waits to expire.
	pub const fn is_finished(self) -> bool {
		matches!(self, Self::Completed | Self::Refused | Self::Failed)
	}

	/// Still waiting for a runner to pick the job up.
	const fn awaits_runner(self) -> bool {
		matches!(self, Self::Queued | Self::Waiting | Self::Starting)
	}
}

/// GitHub's status of a job; it only moves forward.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GithubStatus {
	Queued,
	InProgress,
	Completed,
}

/// One workflow job.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CiJobRow {
	pub job_id:          u64,
	pub run_id:          u64,
	pub run_attempt:     u32,
	pub installation_id: u64,
	pub account:         String,
	/// `owner/name`.
	pub repo:            String,
	pub private:         bool,
	/// GitHub's repository id (webhook payload; `None` on records older than
	/// the CI cache).
	#[serde(default)]
	pub repo_id:         Option<u64>,
	/// The repository's default branch when the job was queued.
	#[serde(default)]
	pub default_branch:  Option<String>,
	/// The job's CI cache scope, resolved by the controller from the job's
	/// workflow run ([`crate::ci::cache`]).
	#[serde(default)]
	pub cache_scope:     Option<CacheScope>,
	pub workflow:        String,
	pub job:             String,
	#[serde(default)]
	pub branch:          Option<String>,
	pub sha:             String,
	pub html_url:        String,
	pub labels:          Vec<String>,
	/// Parsed vmon label (`None`: refused labels).
	#[serde(default)]
	pub spec:            Option<RunnerSpec>,
	pub github:          GithubStatus,
	pub state:           JobState,
	#[serde(default)]
	pub conclusion:      Option<String>,
	#[serde(default)]
	pub reason:          Option<String>,
	/// The account/team/public-repository checks ran.
	#[serde(default)]
	pub decided:         bool,
	#[serde(default)]
	pub team_id:         Option<String>,
	/// vmon runner provisioned for the job, or that ran it.
	#[serde(default)]
	pub runner:          Option<String>,
	/// Runners provisioned for the job so far (names `vmon-<job>-<n>`).
	#[serde(default)]
	pub provisions:      u32,
	/// Runner GitHub reported running the job (vmon's or not).
	#[serde(default)]
	pub runner_name:     Option<String>,
	#[serde(default)]
	pub sandbox_id:      Option<String>,
	#[serde(default)]
	pub node_id:         Option<String>,
	pub queued_at:       DateTime<Utc>,
	#[serde(default)]
	pub started_at:      Option<DateTime<Utc>>,
	#[serde(default)]
	pub completed_at:    Option<DateTime<Utc>>,
	#[serde(default)]
	pub held_until:      Option<DateTime<Utc>>,
	pub updated_at:      DateTime<Utc>,
}

/// Runner states.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum RunnerState {
	/// No sandbox; provisioned at `next_try_at`.
	Pending,
	/// A claim is creating the sandbox.
	Creating,
	/// Sandbox up; JIT registration and runner start (retried at
	/// `next_try_at`).
	Registering,
	/// Runner process started, no job yet (since `idle_since`).
	Idle,
	/// Running `job_id`.
	Busy,
	/// Debug hold until `held_until`.
	Held,
	/// Being torn down (`stop`).
	Stopping,
}

impl RunnerState {
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Pending => "pending",
			Self::Creating => "creating",
			Self::Registering => "registering",
			Self::Idle => "idle",
			Self::Busy => "busy",
			Self::Held => "held",
			Self::Stopping => "stopping",
		}
	}
}

/// Why a runner is torn down.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StopReason {
	/// Its job completed.
	Completed,
	/// Its job ran elsewhere or was cancelled before it started.
	Surplus,
	/// No job within the idle limit.
	Idle,
	/// Reached the runtime cap.
	Expired,
	/// The debug hold ended.
	Released,
	/// Its sandbox ended underneath it.
	Gone,
	/// Provisioning gave up.
	Failed,
	/// A job vmon did not allow started on it.
	Stolen,
}

impl StopReason {
	/// A busy GitHub runner keeps running instead (its job decides). Every
	/// other reason is terminal: the sandbox is killed first, whatever GitHub
	/// says.
	pub const fn yields_to_busy(self) -> bool {
		matches!(self, Self::Surplus | Self::Idle)
	}
}

/// One just-in-time runner and its sandbox.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CiRunnerRow {
	pub name:            String,
	pub installation_id: u64,
	pub account:         String,
	pub repo:            String,
	pub team_id:         String,
	pub spec:            RunnerSpec,
	/// Job the runner was provisioned for (`None`: surplus).
	#[serde(default)]
	pub for_job:         Option<u64>,
	/// Job the runner picked up.
	#[serde(default)]
	pub job_id:          Option<u64>,
	pub state:           RunnerState,
	#[serde(default)]
	pub stop:            Option<StopReason>,
	/// Claim counter: a provisioning step commits only under its attempt.
	#[serde(default)]
	pub attempt:         u32,
	#[serde(default)]
	pub claimed_by:      Option<u64>,
	#[serde(default)]
	pub claimed_at:      Option<DateTime<Utc>>,
	/// Failed provisioning attempts (capacity waits do not count).
	#[serde(default)]
	pub failures:        u32,
	#[serde(default)]
	pub reason:          Option<String>,
	#[serde(default)]
	pub next_try_at:     Option<DateTime<Utc>>,
	#[serde(default)]
	pub sandbox_id:      Option<String>,
	#[serde(default)]
	pub node_id:         Option<String>,
	/// GitHub's runner id once registered.
	#[serde(default)]
	pub runner_id:       Option<u64>,
	pub created_at:      DateTime<Utc>,
	#[serde(default)]
	pub idle_since:      Option<DateTime<Utc>>,
	#[serde(default)]
	pub held_until:      Option<DateTime<Utc>>,
	pub updated_at:      DateTime<Utc>,
}

/// Redeliveries the controller requested for one failed delivery.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CiRedeliveryRow {
	pub attempts: u32,
}

/// Journal record of a job change.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CiEvent {
	pub state:      JobState,
	#[serde(default)]
	pub message:    Option<String>,
	#[serde(default)]
	pub runner:     Option<String>,
	#[serde(default)]
	pub sandbox_id: Option<String>,
}

/// Journal record of a received webhook.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CiDelivery {
	pub delivery: String,
	pub event:    String,
	#[serde(default)]
	pub action:   Option<String>,
	pub outcome:  String,
}

/// A `workflow_job` webhook (or a poll of the job) to apply.
#[derive(Clone, Debug)]
pub struct JobUpdate {
	pub status:       GithubStatus,
	/// The record a first sighting creates (`state` queued or refused).
	pub job:          CiJobRow,
	pub runner_name:  Option<String>,
	pub conclusion:   Option<String>,
	pub started_at:   Option<DateTime<Utc>>,
	pub completed_at: Option<DateTime<Utc>>,
}

/// What [`Store::record_ci_job`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Recorded {
	New,
	Updated,
	/// GitHub's status did not move forward: nothing changed.
	Duplicate,
}

/// Controller verdict on a queued job.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Decision {
	Refuse(String),
	/// Provision a runner in `team_id`.
	Provision {
		team_id: String,
	},
}

/// One step of a runner ([`Store::step_ci_runner`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RunnerStep {
	/// The sandbox exists.
	Created { sandbox_id: String, node_id: String },
	/// GitHub registered the JIT runner.
	Registered { runner_id: u64 },
	/// The runner process runs.
	Started,
	/// Try again at `retry_at` (`failure`: counts towards giving up).
	Retry { reason: String, retry_at: DateTime<Utc>, failure: bool },
	/// Give up: the job is refused (`refuse`) or failed with `reason`.
	Abandon { refuse: bool, reason: String },
	/// Tear the runner down.
	Stop(StopReason),
	/// Teardown found the GitHub runner busy: keep it.
	Revive,
	/// Teardown finished: forget the runner.
	Removed,
}

fn app_key() -> String {
	key(APPS, &[APP])
}

fn manifest_key(state: &str) -> String {
	key(MANIFESTS, &[state])
}

fn account_key(installation_id: u64) -> String {
	key(ACCOUNTS, &[&installation_id.to_string()])
}

fn job_key(job_id: u64) -> String {
	key(JOBS, &[&seq(job_id)])
}

fn active_key(job_id: u64) -> String {
	key(ACTIVE_JOBS, &[&seq(job_id)])
}

fn runner_key(name: &str) -> String {
	key(RUNNERS, &[name])
}

fn redelivery_key(guid: &str) -> String {
	key(REDELIVERIES, &[guid])
}

fn accepted_key(guid: &str) -> String {
	key(ACCEPTED, &[guid])
}

/// Journal stream of one job's history.
pub fn job_stream(job_id: u64) -> String {
	format!("ci/job/{job_id}")
}

/// Name of the `n`th runner provisioned for `job_id`.
pub fn runner_name(job_id: u64, n: u32) -> String {
	format!("vmon-{job_id}-{n}")
}

/// Stages `job` (TTL once finished, active index otherwise) and its history
/// record when its state or reason changed.
fn stage_job(tx: &mut Tx<'_>, before: Option<&CiJobRow>, job: &CiJobRow) {
	if job.state.is_finished() {
		tx.put_ttl(job_key(job.job_id), job, JOB_RETENTION);
		tx.delete(active_key(job.job_id));
	} else {
		tx.put(job_key(job.job_id), job);
		tx.mark(active_key(job.job_id), None);
	}
	let changed = before.is_none_or(|before| {
		before.state != job.state
			|| before.reason != job.reason
			|| before.runner != job.runner
			|| before.sandbox_id != job.sandbox_id
	});
	if changed {
		let message = match job.state {
			JobState::Completed => job.conclusion.clone(),
			_ => job.reason.clone(),
		};
		tx.append(vec![job_stream(job.job_id)], &CiEvent {
			state: job.state,
			message,
			runner: job.runner.clone(),
			sandbox_id: job.sandbox_id.clone(),
		});
	}
}

/// A job waiting for `runner` mirrors its provisioning progress.
fn follow(job: &mut CiJobRow, runner: &CiRunnerRow) {
	if !job.state.awaits_runner() {
		return;
	}
	job.runner = Some(runner.name.clone());
	job.sandbox_id.clone_from(&runner.sandbox_id);
	job.node_id.clone_from(&runner.node_id);
	job.state = match runner.state {
		RunnerState::Pending | RunnerState::Creating if runner.reason.is_some() => JobState::Waiting,
		RunnerState::Pending | RunnerState::Creating => JobState::Queued,
		RunnerState::Registering if runner.reason.is_some() => JobState::Waiting,
		_ => JobState::Starting,
	};
	job.reason.clone_from(&runner.reason);
}

/// A runner nobody waits for any more: tear it down (a busy GitHub runner
/// survives; [`StopReason::yields_to_busy`]).
const fn release(runner: &mut CiRunnerRow, now: DateTime<Utc>) {
	runner.for_job = None;
	if matches!(
		runner.state,
		RunnerState::Pending | RunnerState::Creating | RunnerState::Registering | RunnerState::Idle
	) {
		runner.state = RunnerState::Stopping;
		runner.stop = Some(StopReason::Surplus);
	}
	runner.updated_at = now;
}

/// Rows of one job transition.
struct Touched {
	job:      CiJobRow,
	/// The runner GitHub says runs the job (vmon's).
	ran_on:   Option<CiRunnerRow>,
	/// The job `ran_on` was provisioned for, when another one.
	orphan:   Option<CiJobRow>,
	/// The job's own provisioning runner, when not `ran_on`.
	own:      Option<CiRunnerRow>,
	/// `orphan` as read (its history record compares against it).
	orphan_0: Option<CiJobRow>,
}

/// `job` started on vmon runner `ran_on` although vmon refused it. The runner
/// is stopped and its sandbox killed (its job-started hook fails the job
/// too); the job it was provisioned for (`orphan`) waits for a fresh runner.
/// A job vmon has not decided yet (its `queued` webhook was lost) is adopted
/// instead ([`start_on`]): GitHub matched it to the runner's labels and the
/// runner's job gate checked its repository and actors.
fn steal(t: &mut Touched, now: DateTime<Utc>) {
	let Some(runner) = t.ran_on.as_mut() else {
		return;
	};
	let job = &mut t.job;
	runner.job_id = Some(job.job_id);
	runner.for_job = None;
	runner.state = RunnerState::Stopping;
	runner.stop = Some(StopReason::Stolen);
	runner.idle_since = None;
	runner.updated_at = now;
	let why = job.reason.clone().unwrap_or_else(|| "refused".to_owned());
	job.state = JobState::Refused;
	job.reason = Some(format!(
		"started on vmon runner {} without being allowed ({why}); the runner was stopped",
		runner.name
	));
	job.runner = None;
	job.sandbox_id = None;
	job.node_id = None;
	if let Some(orphan) = t
		.orphan
		.as_mut()
		.filter(|orphan| orphan.state.awaits_runner())
	{
		orphan.runner = None;
		orphan.sandbox_id = None;
		orphan.node_id = None;
		orphan.state = JobState::Queued;
		orphan.reason = None;
	}
	if let Some(own) = t.own.as_mut() {
		release(own, now);
	}
}

/// `job` started on `runner`: the runner's previous job (`orphan`) inherits
/// the job's own provisioning runner (`own`), or gets a fresh one.
fn start_on(t: &mut Touched, now: DateTime<Utc>) {
	let job = &mut t.job;
	let Some(runner) = t.ran_on.as_mut() else {
		if let Some(own) = t.own.as_mut() {
			release(own, now);
		}
		job.runner = None;
		job.sandbox_id = None;
		job.node_id = None;
		return;
	};
	runner.state = RunnerState::Busy;
	runner.job_id = Some(job.job_id);
	runner.for_job = Some(job.job_id);
	runner.idle_since = None;
	runner.reason = None;
	runner.next_try_at = None;
	runner.updated_at = now;
	job.runner = Some(runner.name.clone());
	job.sandbox_id.clone_from(&runner.sandbox_id);
	job.node_id.clone_from(&runner.node_id);
	job.team_id = Some(runner.team_id.clone());
	match (t.orphan.as_mut(), t.own.as_mut()) {
		(Some(orphan), Some(own))
			if orphan.state.awaits_runner()
				&& matches!(
					own.state,
					RunnerState::Pending
						| RunnerState::Creating
						| RunnerState::Registering
						| RunnerState::Idle
				) =>
		{
			own.for_job = Some(orphan.job_id);
			own.updated_at = now;
			follow(orphan, own);
		},
		(orphan, own) => {
			if let Some(orphan) = orphan
				&& orphan.state.awaits_runner()
			{
				orphan.runner = None;
				orphan.sandbox_id = None;
				orphan.node_id = None;
				orphan.state = JobState::Queued;
				orphan.reason = None;
			}
			if let Some(own) = own {
				release(own, now);
			}
		},
	}
}

impl Store {
	// ---------------------------------------------------------------- app

	/// The configured GitHub App.
	pub async fn ci_app(&self) -> Result<Option<CiAppRow>> {
		self.read().await?.get(&app_key()).await
	}

	pub async fn put_ci_app(&self, row: &CiAppRow) -> Result<()> {
		let mut tx = self.tx().await?;
		loop {
			tx.put(app_key(), row);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// Removes the app; `false` when none was configured.
	pub async fn delete_ci_app(&self) -> Result<bool> {
		let mut tx = self.tx().await?;
		loop {
			if tx.get::<CiAppRow>(&app_key()).await?.is_none() {
				return Ok(false);
			}
			tx.delete(app_key());
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	pub async fn put_ci_manifest(&self, state: &str, row: &CiManifestRow) -> Result<()> {
		let mut tx = self.tx().await?;
		loop {
			tx.put_ttl(manifest_key(state), row, MANIFEST_TTL);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// Consumes a pending manifest flow.
	pub async fn take_ci_manifest(&self, state: &str) -> Result<Option<CiManifestRow>> {
		let mut tx = self.tx().await?;
		loop {
			let Some(row) = tx.get::<CiManifestRow>(&manifest_key(state)).await? else {
				return Ok(None);
			};
			tx.delete(manifest_key(state));
			if tx.commit().await? {
				return Ok(Some(row));
			}
		}
	}

	// ----------------------------------------------------------- accounts

	pub async fn ci_accounts(&self) -> Result<Vec<CiAccountRow>> {
		self.read().await?.list(&prefix(ACCOUNTS, &[])).await
	}

	pub async fn ci_account(&self, installation_id: u64) -> Result<Option<CiAccountRow>> {
		self.read().await?.get(&account_key(installation_id)).await
	}

	/// Records an installation, keeping its team link and policy.
	pub async fn upsert_ci_account(&self, seen: &InstallationSeen) -> Result<CiAccountRow> {
		let mut tx = self.tx().await?;
		loop {
			let row = upsert_account(&mut tx, seen).await?;
			if tx.commit().await? {
				return Ok(row);
			}
		}
	}

	/// Forgets an uninstalled account; `false` when unknown.
	pub async fn delete_ci_account(&self, installation_id: u64) -> Result<bool> {
		let mut tx = self.tx().await?;
		loop {
			if tx
				.get::<CiAccountRow>(&account_key(installation_id))
				.await?
				.is_none()
			{
				return Ok(false);
			}
			tx.delete(account_key(installation_id));
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	/// Makes the accounts exactly `seen` (links and policies of kept
	/// accounts survive).
	pub async fn sync_ci_accounts(&self, seen: &[InstallationSeen]) -> Result<Vec<CiAccountRow>> {
		let mut tx = self.tx().await?;
		loop {
			let current: Vec<CiAccountRow> = tx.list(&prefix(ACCOUNTS, &[])).await?;
			for gone in current.iter().filter(|row| {
				!seen
					.iter()
					.any(|s| s.installation_id == row.installation_id)
			}) {
				tx.delete(account_key(gone.installation_id));
			}
			let mut rows = Vec::with_capacity(seen.len());
			for installation in seen {
				rows.push(upsert_account(&mut tx, installation).await?);
			}
			if tx.commit().await? {
				rows.sort_by_key(|row| row.login.to_lowercase());
				return Ok(rows);
			}
		}
	}

	/// Applies `change` to an account; `None` when unknown.
	pub async fn update_ci_account(
		&self,
		installation_id: u64,
		change: impl Fn(&mut CiAccountRow),
	) -> Result<Option<CiAccountRow>> {
		let mut tx = self.tx().await?;
		loop {
			let Some(mut row) = tx
				.get::<CiAccountRow>(&account_key(installation_id))
				.await?
			else {
				return Ok(None);
			};
			change(&mut row);
			row.updated_at = now();
			tx.put(account_key(installation_id), &row);
			if tx.commit().await? {
				return Ok(Some(row));
			}
		}
	}

	// --------------------------------------------------------------- jobs

	pub async fn ci_job(&self, job_id: u64) -> Result<Option<CiJobRow>> {
		self.read().await?.get(&job_key(job_id)).await
	}

	/// Up to `limit` jobs matching `keep`, newest (highest id) first, after
	/// job `after`.
	pub async fn ci_jobs(
		&self,
		after: Option<u64>,
		limit: usize,
		keep: impl Fn(&CiJobRow) -> bool,
	) -> Result<Vec<CiJobRow>> {
		let range = Range {
			prefix:  prefix(JOBS, &[]),
			after:   after.map(job_key),
			limit:   None,
			reverse: true,
		};
		let mut jobs = Vec::new();
		self
			.read()
			.await?
			.scan_all(&range, |_, job: CiJobRow| {
				if keep(&job) {
					jobs.push(job);
				}
				jobs.len() < limit
			})
			.await?;
		Ok(jobs)
	}

	/// Unfinished jobs, oldest first.
	pub async fn ci_active_jobs(&self) -> Result<Vec<CiJobRow>> {
		let r = self.read().await?;
		let mut jobs = Vec::new();
		for index in r.keys(&prefix(ACTIVE_JOBS, &[])).await? {
			let id = super::last_segment(&index);
			if let Some(job) = r.get::<CiJobRow>(&key(JOBS, &[&id])).await? {
				jobs.push(job);
			}
		}
		Ok(jobs)
	}

	/// History of a job, oldest first.
	pub async fn ci_job_events(&self, job_id: u64) -> Result<Vec<Logged<CiEvent>>> {
		self
			.read()
			.await?
			.journal(&Scan { stream: job_stream(job_id), ..Scan::default() })
			.await
	}

	/// Appends a received webhook to the journal.
	pub async fn log_ci_delivery(&self, delivery: &CiDelivery) -> Result<()> {
		let mut tx = self.tx().await?;
		loop {
			tx.append(vec![DELIVERIES_STREAM.to_owned()], delivery);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// The newest received webhooks, newest first.
	pub async fn ci_deliveries(&self, limit: usize) -> Result<Vec<Logged<CiDelivery>>> {
		self
			.read()
			.await?
			.journal(&Scan {
				stream:  DELIVERIES_STREAM.to_owned(),
				limit:   Some(limit),
				reverse: true,
				after:   None,
			})
			.await
	}

	/// Applies a GitHub job status (webhook or poll). Idempotent: a status
	/// that does not move the job forward changes nothing.
	pub async fn record_ci_job(&self, update: &JobUpdate) -> Result<Recorded> {
		let id = update.job.job_id;
		let mut tx = self.tx().await?;
		loop {
			let now = now();
			let before: Option<CiJobRow> = tx.get(&job_key(id)).await?;
			let job = match &before {
				Some(row) if update.status <= row.github => return Ok(Recorded::Duplicate),
				Some(row) => row.clone(),
				None => update.job.clone(),
			};
			let mut t = Touched { job, ran_on: None, orphan: None, own: None, orphan_0: None };
			t.job.github = update.status;
			t.job.updated_at = now;
			if let Some(name) = update.runner_name.as_ref().filter(|name| !name.is_empty()) {
				t.job.runner_name = Some(name.clone());
			}
			let ran = t.job.runner_name.is_some();
			if update.status == GithubStatus::InProgress || ran {
				let started = update.started_at.unwrap_or(now);
				t.job.started_at.get_or_insert(started);
				let ran_on_ours = match &t.job.runner_name {
					Some(name) => tx.get::<CiRunnerRow>(&runner_key(name)).await?,
					None => None,
				};
				let needs_start = ran_on_ours
					.as_ref()
					.is_some_and(|runner| runner.job_id != Some(id));
				if let Some(runner) = &ran_on_ours
					&& let Some(previous) = runner.for_job.filter(|&job| job != id)
				{
					t.orphan = tx.get(&job_key(previous)).await?;
					t.orphan_0.clone_from(&t.orphan);
				}
				if let Some(own) = t
					.job
					.runner
					.clone()
					.filter(|own| Some(own) != t.job.runner_name.as_ref())
				{
					t.own = tx
						.get::<CiRunnerRow>(&runner_key(&own))
						.await?
						.filter(|runner| runner.for_job == Some(id));
				}
				t.ran_on = ran_on_ours;
				if needs_start && t.job.state == JobState::Refused {
					steal(&mut t, now);
				} else if needs_start || t.ran_on.is_none() {
					start_on(&mut t, now);
				}
				if !t.job.state.is_finished() && t.job.state != JobState::Held {
					t.job.state = JobState::Running;
					t.job.reason = None;
				}
			}
			if update.status == GithubStatus::Completed {
				t.job.conclusion.clone_from(&update.conclusion);
				t.job.completed_at = Some(update.completed_at.unwrap_or(now));
				if !ran && let Some(own) = t.job.runner.clone() {
					t.own = tx
						.get::<CiRunnerRow>(&runner_key(&own))
						.await?
						.filter(|runner| runner.for_job == Some(id));
					if let Some(own) = t.own.as_mut() {
						release(own, now);
					}
				}
				let finished = t.job.state.is_finished();
				match t.ran_on.as_mut() {
					Some(runner)
						if !finished
							&& runner.spec.debug
							&& update.conclusion.as_deref() == Some("failure") =>
					{
						let until = now + HOLD;
						runner.state = RunnerState::Held;
						runner.held_until = Some(until);
						runner.updated_at = now;
						t.job.state = JobState::Held;
						t.job.held_until = Some(until);
					},
					Some(runner) => {
						if runner.state != RunnerState::Stopping
							|| runner.stop.is_some_and(StopReason::yields_to_busy)
						{
							runner.state = RunnerState::Stopping;
							runner.stop = Some(StopReason::Completed);
							runner.updated_at = now;
						}
						if !finished {
							t.job.state = JobState::Completed;
						}
					},
					None if !finished => t.job.state = JobState::Completed,
					None => {},
				}
				if !ran {
					t.job.sandbox_id = None;
					t.job.node_id = None;
				}
			}
			stage_job(&mut tx, before.as_ref(), &t.job);
			for runner in [&t.ran_on, &t.own].into_iter().flatten() {
				tx.put(runner_key(&runner.name), runner);
			}
			if let Some(orphan) = &t.orphan {
				stage_job(&mut tx, t.orphan_0.as_ref(), orphan);
			}
			if tx.commit().await? {
				return Ok(if before.is_some() {
					Recorded::Updated
				} else {
					Recorded::New
				});
			}
		}
	}

	/// Applies the controller's verdict on a queued job without a runner;
	/// provisioning names and records a fresh runner. `None` when the job no
	/// longer waits for one (handled, started, or gone).
	pub async fn decide_ci_job(&self, job_id: u64, decision: &Decision) -> Result<Option<CiJobRow>> {
		let mut tx = self.tx().await?;
		loop {
			let now = now();
			let Some(before) = tx.get::<CiJobRow>(&job_key(job_id)).await? else {
				return Ok(None);
			};
			let waiting = before.state == JobState::Queued
				&& before.github == GithubStatus::Queued
				&& before.runner.is_none();
			let Some(spec) = before.spec.clone().filter(|_| waiting) else {
				return Ok(None);
			};
			let mut job = before.clone();
			job.decided = true;
			job.updated_at = now;
			match decision {
				Decision::Refuse(reason) => {
					job.state = JobState::Refused;
					job.reason = Some(reason.clone());
				},
				Decision::Provision { team_id } => {
					// Numbers already taken (a runner still alive from an
					// earlier provisioning) are skipped.
					let name = loop {
						job.provisions += 1;
						let name = runner_name(job_id, job.provisions);
						if tx.get::<CiRunnerRow>(&runner_key(&name)).await?.is_none() {
							break name;
						}
					};
					let runner = CiRunnerRow {
						name: name.clone(),
						installation_id: job.installation_id,
						account: job.account.clone(),
						repo: job.repo.clone(),
						team_id: team_id.clone(),
						spec,
						for_job: Some(job_id),
						job_id: None,
						state: RunnerState::Pending,
						stop: None,
						attempt: 0,
						claimed_by: None,
						claimed_at: None,
						failures: 0,
						reason: None,
						next_try_at: None,
						sandbox_id: None,
						node_id: None,
						runner_id: None,
						created_at: now,
						idle_since: None,
						held_until: None,
						updated_at: now,
					};
					job.team_id = Some(team_id.clone());
					job.runner = Some(name.clone());
					tx.put(runner_key(&name), &runner);
				},
			}
			stage_job(&mut tx, Some(&before), &job);
			if tx.commit().await? {
				return Ok(Some(job));
			}
		}
	}

	/// Records the cache scope the controller resolved for a job. `false`:
	/// the job is gone.
	pub async fn set_ci_job_cache_scope(&self, job_id: u64, scope: &CacheScope) -> Result<bool> {
		let mut tx = self.tx().await?;
		loop {
			let Some(before) = tx.get::<CiJobRow>(&job_key(job_id)).await? else {
				return Ok(false);
			};
			if before.cache_scope.as_ref() == Some(scope) {
				return Ok(true);
			}
			let mut job = before.clone();
			job.cache_scope = Some(scope.clone());
			stage_job(&mut tx, Some(&before), &job);
			if tx.commit().await? {
				return Ok(true);
			}
		}
	}

	// ------------------------------------------------------------ runners

	pub async fn ci_runners(&self) -> Result<Vec<CiRunnerRow>> {
		self.read().await?.list(&prefix(RUNNERS, &[])).await
	}

	pub async fn ci_runner(&self, name: &str) -> Result<Option<CiRunnerRow>> {
		self.read().await?.get(&runner_key(name)).await
	}

	/// Claims a runner due for provisioning (pending or registering past
	/// `next_try_at`, or a claim older than `stale`) for driver `by`: bumps
	/// its attempt. `None` when it is not due.
	pub async fn claim_ci_runner(
		&self,
		name: &str,
		by: u64,
		stale: Duration,
	) -> Result<Option<CiRunnerRow>> {
		let mut tx = self.tx().await?;
		loop {
			let now = now();
			let Some(mut runner) = tx.get::<CiRunnerRow>(&runner_key(name)).await? else {
				return Ok(None);
			};
			let due = runner.next_try_at.is_none_or(|at| at <= now);
			let free = runner
				.claimed_at
				.is_none_or(|at| at + stale <= now || runner.claimed_by.is_none());
			let claimable = match runner.state {
				RunnerState::Pending | RunnerState::Registering => due && free,
				RunnerState::Creating => free,
				_ => false,
			};
			if !claimable {
				return Ok(None);
			}
			if runner.state == RunnerState::Pending {
				runner.state = RunnerState::Creating;
			}
			runner.attempt += 1;
			runner.claimed_by = Some(by);
			runner.claimed_at = Some(now);
			runner.updated_at = now;
			tx.put(runner_key(name), &runner);
			if tx.commit().await? {
				return Ok(Some(runner));
			}
		}
	}

	/// Applies `step` to a runner and the job waiting on it. With `attempt`,
	/// only while that claim is current (`None`: lost or gone). Returns the
	/// updated runner (`Removed`: the removed one).
	pub async fn step_ci_runner(
		&self,
		name: &str,
		attempt: Option<u32>,
		step: &RunnerStep,
	) -> Result<Option<CiRunnerRow>> {
		let mut tx = self.tx().await?;
		loop {
			let now = now();
			let Some(mut runner) = tx.get::<CiRunnerRow>(&runner_key(name)).await? else {
				return Ok(None);
			};
			if attempt.is_some_and(|attempt| attempt != runner.attempt) {
				return Ok(None);
			}
			let job_id = runner.job_id.or(runner.for_job);
			let before = match job_id {
				Some(id) => tx.get::<CiJobRow>(&job_key(id)).await?,
				None => None,
			};
			let mut job = before.clone();
			runner.updated_at = now;
			let mut removed = false;
			match step {
				RunnerStep::Created { sandbox_id, node_id } => {
					if runner.state != RunnerState::Creating {
						return Ok(None);
					}
					runner.state = RunnerState::Registering;
					runner.sandbox_id = Some(sandbox_id.clone());
					runner.node_id = Some(node_id.clone());
					runner.reason = None;
					runner.next_try_at = None;
				},
				RunnerStep::Registered { runner_id } => runner.runner_id = Some(*runner_id),
				RunnerStep::Started => {
					if runner.state != RunnerState::Registering {
						return Ok(None);
					}
					runner.state = RunnerState::Idle;
					runner.idle_since = Some(now);
					runner.reason = None;
					runner.next_try_at = None;
					runner.claimed_by = None;
					runner.claimed_at = None;
				},
				RunnerStep::Retry { reason, retry_at, failure } => {
					if runner.state == RunnerState::Creating {
						runner.state = RunnerState::Pending;
					}
					runner.reason = Some(reason.clone());
					runner.next_try_at = Some(*retry_at);
					runner.failures += u32::from(*failure);
					runner.claimed_by = None;
					runner.claimed_at = None;
				},
				RunnerStep::Abandon { refuse, reason } => {
					if let Some(job) = job.as_mut().filter(|job| job.state.awaits_runner()) {
						job.state = if *refuse {
							JobState::Refused
						} else {
							JobState::Failed
						};
						job.reason = Some(reason.clone());
						job.decided = true;
					}
					runner.for_job = None;
					runner.state = RunnerState::Stopping;
					runner.stop = Some(StopReason::Failed);
					runner.reason = Some(reason.clone());
				},
				RunnerStep::Stop(stop) => {
					// A stopping runner only moves from a surplus/idle stop to
					// a terminal one (e.g. the runtime cap).
					let upgrade =
						runner.stop.is_some_and(StopReason::yields_to_busy) && !stop.yields_to_busy();
					if runner.state == RunnerState::Stopping && !upgrade {
						return Ok(Some(runner));
					}
					if let Some(job) = job.as_mut().filter(|job| job.state == JobState::Running) {
						match stop {
							StopReason::Expired => {
								job.state = JobState::Failed;
								job.reason = Some("the job exceeded the runner's time limit".into());
							},
							StopReason::Gone => {
								job.state = JobState::Failed;
								job.reason = Some("the runner's sandbox ended during the job".into());
							},
							_ => {},
						}
					}
					runner.state = RunnerState::Stopping;
					runner.stop = Some(*stop);
				},
				RunnerStep::Revive => {
					// Only a surplus/idle stop yields to a busy runner; a
					// terminal stop recorded meanwhile stands.
					if runner.state != RunnerState::Stopping
						|| !runner.stop.is_some_and(StopReason::yields_to_busy)
					{
						return Ok(Some(runner));
					}
					runner.state = RunnerState::Idle;
					runner.stop = None;
					runner.idle_since = Some(now);
				},
				RunnerStep::Removed => {
					removed = true;
					if let Some(job) = job.as_mut() {
						if job.state == JobState::Held {
							job.state = JobState::Completed;
						} else if job.state.awaits_runner() && job.runner.as_deref() == Some(name) {
							// Provision again.
							job.runner = None;
							job.sandbox_id = None;
							job.node_id = None;
							job.state = JobState::Queued;
							job.reason.clone_from(&runner.reason);
						}
					}
				},
			}
			if let Some(job) = job.as_mut() {
				if !removed && job.runner.as_deref() == Some(name) {
					follow(job, &runner);
				}
				job.updated_at = now;
				stage_job(&mut tx, before.as_ref(), job);
			}
			if removed {
				tx.delete(runner_key(name));
			} else {
				tx.put(runner_key(name), &runner);
			}
			if tx.commit().await? {
				return Ok(Some(runner));
			}
		}
	}

	// -------------------------------------------------------- redelivery

	/// Records that vmon answered webhook delivery `guid` with 2xx.
	pub async fn mark_ci_delivery_accepted(&self, guid: &str) -> Result<()> {
		let mut tx = self.tx().await?;
		loop {
			tx.put_ttl(accepted_key(guid), &true, ACCEPTED_TTL);
			if tx.commit().await? {
				return Ok(());
			}
		}
	}

	/// Whether vmon answered webhook delivery `guid` with 2xx (last 25 h).
	pub async fn ci_delivery_accepted(&self, guid: &str) -> Result<bool> {
		Ok(self
			.read()
			.await?
			.get::<bool>(&accepted_key(guid))
			.await?
			.is_some())
	}

	/// Counts one more requested redelivery of webhook delivery `guid`;
	/// returns the count before it.
	pub async fn count_ci_redelivery(&self, guid: &str) -> Result<u32> {
		let mut tx = self.tx().await?;
		loop {
			let mut row: CiRedeliveryRow = tx.get(&redelivery_key(guid)).await?.unwrap_or_default();
			let before = row.attempts;
			row.attempts += 1;
			tx.put_ttl(redelivery_key(guid), &row, REDELIVERY_TTL);
			if tx.commit().await? {
				return Ok(before);
			}
		}
	}
}

async fn upsert_account(tx: &mut Tx<'_>, seen: &InstallationSeen) -> Result<CiAccountRow> {
	let now = now();
	let row = match tx
		.get::<CiAccountRow>(&account_key(seen.installation_id))
		.await?
	{
		Some(mut row) => {
			row.login.clone_from(&seen.login);
			row.kind.clone_from(&seen.kind);
			row.suspended = seen.suspended;
			if seen.permissions.is_some() {
				row.permissions.clone_from(&seen.permissions);
			}
			row.updated_at = now;
			row
		},
		None => CiAccountRow {
			installation_id: seen.installation_id,
			login:           seen.login.clone(),
			kind:            seen.kind.clone(),
			team_id:         None,
			suspended:       seen.suspended,
			public:          PublicPolicy::default(),
			permissions:     seen.permissions.clone(),
			created_at:      now,
			updated_at:      now,
		},
	};
	tx.put(account_key(seen.installation_id), &row);
	Ok(row)
}

#[cfg(test)]
mod tests;
