//! Leader-only CI controller.
//!
//! Every [`TICK`] (and right after a webhook) the raft leader:
//! - decides queued jobs without a runner: refused with a reason, or a runner
//!   record is created ([`decide`]);
//! - provisions due runners: sandbox → JIT config → runner process
//!   ([`provision`]); capacity and rate limits retry every [`RETRY`];
//! - tears runners down ([`teardown`]) after their job, after the debug hold,
//!   when idle for [`IDLE_LIMIT`], or at [`RUNTIME_LIMIT`].
//!
//! Every [`RECONCILE`] it also redelivers failed webhook deliveries, polls
//! jobs whose webhooks seem lost, stops runners whose sandbox ended, and
//! reaps sandboxes (by their `vmon.ci.runner` metadata) and GitHub runners no
//! runner record owns.
//!
//! Work items run as tasks keyed by job or runner, at most one per key on
//! this driver; across leader changes the store's claims (`attempt`) keep
//! one sandbox per runner record.

use std::{
	collections::{BTreeMap, BTreeSet, HashMap},
	future::Future,
	time::Duration,
};

use axum::http::StatusCode;
use chrono::Utc;
use vmon_node::api::StartProcess;

use super::{
	gate,
	github::{Delivery, Github, GithubError},
};
use crate::{
	error::ApiError,
	lifecycle::{
		self, CreateSpec, KillReason, LeaderTicks, Resources, limits, network::NetworkConfig, routes,
	},
	state::AppState,
	store::{
		self,
		ci::{
			CiJobRow, CiRunnerRow, Decision, GithubStatus, InstallationSeen, JobState, RunnerState,
			RunnerStep, StopReason,
		},
		sandboxes::{ListOrder, SandboxFilter, SandboxState},
	},
};

/// Controller pass period.
const TICK: Duration = Duration::from_secs(1);
/// Reconcile period.
const RECONCILE: Duration = Duration::from_mins(1);
/// Retry period of capacity, rate-limit, and transient failures.
pub(super) const RETRY: Duration = Duration::from_secs(15);
/// An idle runner (no job) is removed after this long.
pub(super) const IDLE_LIMIT: Duration = Duration::from_mins(10);
/// Runtime cap of a runner.
pub(super) const RUNTIME_LIMIT: Duration = Duration::from_hours(6);
/// Sandbox timeout: the runtime cap plus teardown slack.
const SANDBOX_TIMEOUT: Duration = Duration::from_mins(6 * 60 + 10);
/// A provisioning claim older than this is taken over (a sandbox create
/// tries up to three nodes at 5 minutes each).
const CLAIM_STALE: Duration = Duration::from_mins(16);
/// Failed provisioning attempts before a job fails.
const MAX_FAILURES: u32 = 3;
/// A CI sandbox no runner record owns is killed once this old.
const ORPHAN_GRACE: Duration = Duration::from_mins(2);
/// Unfinished jobs queued this long ago are polled from GitHub (a lost
/// `completed` webhook must not keep a job waiting for capacity forever).
const POLL_AFTER: Duration = Duration::from_mins(2);
/// Jobs polled per reconcile.
const POLL_BATCH: usize = 30;
/// Repositories whose GitHub runners are checked per reconcile.
const RUNNER_REPOS: usize = 10;
/// Requested redeliveries per failed delivery.
const MAX_REDELIVERIES: u32 = 3;
/// Failed deliveries older than this are left alone.
const REDELIVER_WINDOW: chrono::Duration = chrono::Duration::hours(24);
/// Pages of failed deliveries (100 each) read per reconcile.
const REDELIVER_PAGES: u32 = 10;
/// Directory of the runner in the `ci` image, and its user.
const RUNNER_DIR: &str = "/home/user/actions-runner";
const RUNNER_USER: &str = "user";
/// Sandbox metadata keys.
pub const META_JOB: &str = "vmon.ci.job";
pub const META_REPO: &str = "vmon.ci.repo";
pub const META_RUNNER: &str = "vmon.ci.runner";
/// Environment variable carrying the JIT config into the start wrapper
/// (keeps it out of the logged command line).
const JIT_ENV: &str = "VMON_CI_JITCONFIG";
/// Environment variable carrying the job gate script into the start wrapper.
const GATE_ENV: &str = "VMON_CI_GATE";

/// Starts the controller loop (every driver; only the leader works).
pub fn spawn_controller(state: AppState) {
	tokio::spawn(run(state));
}

async fn run(state: AppState) {
	let mut ticks = LeaderTicks::new(&state, TICK);
	let mut last_reconcile: Option<tokio::time::Instant> = None;
	loop {
		let leader = tokio::select! {
			tick = ticks.tick() => match tick {
				Some(leader) => leader,
				None => return,
			},
			() = state.ci.wake.notified() => state.cluster.is_leader(),
		};
		if !leader {
			last_reconcile = None;
			continue;
		}
		if let Err(error) = pass(&state).await {
			tracing::warn!(error = %error.message, "CI controller pass failed");
		}
		if last_reconcile.is_none_or(|at| at.elapsed() >= RECONCILE) {
			last_reconcile = Some(tokio::time::Instant::now());
			let task_state = state.clone();
			spawn_once(&state, "reconcile".to_owned(), async move {
				if let Err(error) = reconcile(&task_state).await {
					tracing::warn!(error = %error.message, "CI reconcile failed");
				}
			});
		}
	}
}

/// Runs `task` unless a task with `key` is in flight on this driver.
fn spawn_once(state: &AppState, key: String, task: impl Future<Output = ()> + Send + 'static) {
	if !state.ci.inflight.lock().insert(key.clone()) {
		return;
	}
	let state = state.clone();
	tokio::spawn(async move {
		task.await;
		state.ci.inflight.lock().remove(&key);
	});
}

fn runner_key(name: &str) -> String {
	format!("runner:{name}")
}

fn chrono(duration: Duration) -> chrono::Duration {
	chrono::Duration::from_std(duration).unwrap_or(chrono::Duration::MAX)
}

/// One controller pass (see the module docs).
pub(super) async fn pass(state: &AppState) -> Result<(), ApiError> {
	// `Err`: the app exists but is unusable right now (teardown must not
	// drop a runner record without its GitHub cleanup then).
	let github = match Github::load(state).await {
		Ok(github) => Ok(github.map(std::sync::Arc::new)),
		Err(error) => {
			tracing::warn!(error = %error.message, "CI GitHub App unusable");
			Err(error.message)
		},
	};
	if let Ok(Some(github)) = &github {
		for job in state.store.ci_active_jobs().await? {
			let waits = job.state == JobState::Queued
				&& job.github == GithubStatus::Queued
				&& job.runner.is_none()
				&& job.spec.is_some();
			if waits {
				let (task_state, github) = (state.clone(), github.clone());
				spawn_once(state, format!("job:{}", job.job_id), async move {
					decide(&task_state, &github, &job).await;
				});
			}
		}
	}
	let now = Utc::now();
	for runner in state.store.ci_runners().await? {
		let name = runner.name.clone();
		let expired = runner.created_at + chrono(RUNTIME_LIMIT) <= now;
		let stop = match runner.state {
			// A surplus or idle runner GitHub keeps refusing to delete must
			// still go at the runtime cap.
			RunnerState::Stopping
				if expired && runner.stop.is_some_and(StopReason::yields_to_busy) =>
			{
				Some(StopReason::Expired)
			},
			RunnerState::Stopping => None,
			RunnerState::Held if runner.held_until.is_some_and(|until| until <= now) => {
				Some(StopReason::Released)
			},
			RunnerState::Held => {
				let (task_state, held) = (state.clone(), runner.clone());
				spawn_once(state, runner_key(&name), async move {
					keep_held(&task_state, &held).await;
				});
				continue;
			},
			_ if expired => Some(StopReason::Expired),
			RunnerState::Idle
				if runner
					.idle_since
					.is_some_and(|since| since + chrono(IDLE_LIMIT) <= now) =>
			{
				Some(StopReason::Idle)
			},
			RunnerState::Pending | RunnerState::Creating | RunnerState::Registering => {
				let due = runner.next_try_at.is_none_or(|at| at <= now);
				let stale = runner
					.claimed_at
					.is_none_or(|at| at + chrono(CLAIM_STALE) <= now);
				if (due && runner.state != RunnerState::Creating) || stale {
					let task_state = state.clone();
					spawn_once(state, runner_key(&name), async move {
						provision(&task_state, &name).await;
					});
				}
				continue;
			},
			_ => continue,
		};
		if let Some(stop) = stop {
			state
				.store
				.step_ci_runner(&name, None, &RunnerStep::Stop(stop))
				.await?;
		}
		let (task_state, github) = (state.clone(), github.clone());
		spawn_once(state, runner_key(&name), async move {
			let github = github
				.as_ref()
				.map(Option::as_deref)
				.map_err(String::as_str);
			teardown(&task_state, github, &name).await;
		});
	}
	Ok(())
}

// ------------------------------------------------------------- decisions

/// Why `job` must not run, if it must not.
async fn verdict(state: &AppState, github: &Github, job: &CiJobRow) -> Result<Decision, String> {
	let Some(spec) = &job.spec else {
		return Ok(Decision::Refuse("the job has no vmon label".into()));
	};
	let account = match state.store.ci_account(job.installation_id).await {
		Ok(Some(account)) => account,
		Ok(None) => match github.installation(job.installation_id).await {
			Ok(installation) => state
				.store
				.upsert_ci_account(&InstallationSeen {
					installation_id: installation.id,
					login:           installation.account.login,
					kind:            installation.account.kind,
					suspended:       installation.suspended_at.is_some(),
					permissions:     Some(installation.permissions),
				})
				.await
				.map_err(|error| error.to_string())?,
			Err(error) if error.is_transient() => return Err(error.to_string()),
			Err(error) => {
				return Ok(Decision::Refuse(format!(
					"GitHub App installation {} is unknown: {error}",
					job.installation_id
				)));
			},
		},
		Err(error) => return Err(error.to_string()),
	};
	if account.suspended {
		return Ok(Decision::Refuse(format!("the GitHub App is suspended on {}", account.login)));
	}
	let Some(team_id) = &account.team_id else {
		return Ok(Decision::Refuse(format!(
			"GitHub account {} is not linked to a vmon team (vmon admin ci link {} --team <team>)",
			account.login, account.login
		)));
	};
	let team = match state.store.team_by_id(team_id).await {
		Ok(Some(team)) => team,
		Ok(None) => {
			return Ok(Decision::Refuse(format!(
				"the vmon team linked to {} no longer exists",
				account.login
			)));
		},
		Err(error) => return Err(error.to_string()),
	};
	if team.blocked {
		return Ok(Decision::Refuse(format!("vmon team {} is blocked", team.slug)));
	}
	let limits = team.limits;
	if spec.size.vcpus() > limits.max_vcpus || spec.size.memory_mb() > limits.max_memory_mb {
		return Ok(Decision::Refuse(format!(
			"{} needs {} vCPUs and {} MiB; team {} allows {} vCPUs and {} MiB",
			spec.size,
			spec.size.vcpus(),
			spec.size.memory_mb(),
			team.slug,
			limits.max_vcpus,
			limits.max_memory_mb
		)));
	}
	if !job.private {
		let policy = &account.public;
		if !policy.enabled {
			return Ok(Decision::Refuse(format!(
				"{} is public and public repositories are off for {}",
				job.repo, account.login
			)));
		}
		if !policy.anyone {
			let run = github
				.run(job.installation_id, &job.repo, job.run_id)
				.await
				.map_err(|error| error.to_string())?;
			let actor = run.actor.login;
			let triggering = run
				.triggering_actor
				.map_or_else(|| actor.clone(), |login| login.login);
			if !policy.allows(&actor, &triggering) {
				return Ok(Decision::Refuse(format!(
					"{} is public and the run by @{actor} (triggered by @{triggering}) is not from an \
					 allowed user",
					job.repo
				)));
			}
		}
	}
	Ok(Decision::Provision { team_id: team.id })
}

async fn decide(state: &AppState, github: &Github, job: &CiJobRow) {
	// A decided job only needs a fresh runner.
	let decision = match (&job.team_id, job.decided) {
		(Some(team_id), true) => Ok(Decision::Provision { team_id: team_id.clone() }),
		_ => verdict(state, github, job).await,
	};
	match decision {
		Ok(decision) => match state.store.decide_ci_job(job.job_id, &decision).await {
			Ok(Some(job)) => {
				tracing::info!(job = job.job_id, repo = %job.repo, state = job.state.as_str(), reason = ?job.reason, runner = ?job.runner, "CI job decided");
				state.ci.wake();
			},
			Ok(None) => {},
			Err(error) => tracing::warn!(%error, job = job.job_id, "recording a CI decision failed"),
		},
		Err(error) => {
			tracing::warn!(%error, job = job.job_id, "deciding a CI job failed; retrying");
			tokio::time::sleep(RETRY).await;
		},
	}
}

// ---------------------------------------------------------- provisioning

/// The sandbox of runner `name` an earlier claim created, if any.
async fn find_sandbox(state: &AppState, name: &str) -> Result<Option<(String, String)>, ApiError> {
	let filter = SandboxFilter {
		states: vec![SandboxState::Running],
		metadata: BTreeMap::from([(META_RUNNER.to_owned(), name.to_owned())]),
		..SandboxFilter::default()
	};
	Ok(state
		.store
		.list_sandboxes(&filter, ListOrder::Desc, None, 1)
		.await?
		.into_iter()
		.next()
		.map(|row| (row.id, row.node_id)))
}

/// What a failed provisioning step does next.
fn retry(runner: &CiRunnerRow, reason: String, failure: bool) -> RunnerStep {
	if failure && runner.failures + 1 >= MAX_FAILURES {
		RunnerStep::Abandon { refuse: false, reason }
	} else {
		RunnerStep::Retry { reason, retry_at: store::now() + chrono(RETRY), failure }
	}
}

/// Step for a failed sandbox create.
fn create_failed(runner: &CiRunnerRow, error: &ApiError) -> RunnerStep {
	match error.status {
		StatusCode::TOO_MANY_REQUESTS | StatusCode::SERVICE_UNAVAILABLE => {
			retry(runner, format!("waiting for capacity: {}", error.message), false)
		},
		StatusCode::BAD_REQUEST | StatusCode::FORBIDDEN | StatusCode::NOT_FOUND => {
			RunnerStep::Abandon { refuse: true, reason: error.message.clone() }
		},
		_ => retry(runner, format!("creating the sandbox failed: {}", error.message), true),
	}
}

/// Step for a failed GitHub call.
fn github_failed(runner: &CiRunnerRow, what: &str, error: &GithubError) -> RunnerStep {
	if error.is_transient() {
		retry(runner, format!("{what}: {error}"), !error.rate_limited)
	} else {
		RunnerStep::Abandon { refuse: false, reason: format!("{what}: {error}") }
	}
}

async fn step(state: &AppState, name: &str, attempt: u32, step: &RunnerStep) -> bool {
	match state.store.step_ci_runner(name, Some(attempt), step).await {
		Ok(Some(_)) => {
			state.ci.wake();
			true
		},
		Ok(None) => false,
		Err(error) => {
			tracing::warn!(%error, runner = name, ?step, "recording a CI runner step failed");
			false
		},
	}
}

async fn create_sandbox(
	state: &AppState,
	runner: &CiRunnerRow,
) -> Result<(String, String), RunnerStep> {
	let team = match state.store.team_by_id(&runner.team_id).await {
		Ok(Some(team)) => team,
		Ok(None) => {
			return Err(RunnerStep::Abandon {
				refuse: true,
				reason: format!("vmon team {} no longer exists", runner.team_id),
			});
		},
		Err(error) => {
			return Err(retry(runner, format!("reading the team failed: {error}"), false));
		},
	};
	let max_s = i64::from(team.limits.max_length_hours) * 3600;
	let timeout_s = i64::try_from(SANDBOX_TIMEOUT.as_secs())
		.unwrap_or(i64::MAX)
		.min(max_s);
	let spec = CreateSpec {
		template_ref:          runner
			.spec
			.template
			.clone()
			.unwrap_or_else(|| state.config.ci_template.clone()),
		timeout_ms:            limits::validate_timeout(timeout_s, team.limits.max_length_hours)
			.map_err(|error| create_failed(runner, &error))?,
		flags:                 limits::lifecycle_flags(Some(false), None, Some(false))
			.map_err(|error| create_failed(runner, &error))?,
		allow_internet_access: Some(true),
		network:               NetworkConfig::default(),
		metadata:              BTreeMap::from([
			(META_JOB.to_owned(), runner.for_job.map(|id| id.to_string()).unwrap_or_default()),
			(META_REPO.to_owned(), runner.repo.clone()),
			(META_RUNNER.to_owned(), runner.name.clone()),
		]),
		env_vars:              BTreeMap::new(),
		volume_mounts:         Vec::new(),
		s3_mounts:             Vec::new(),
		resources:             Some(Resources {
			vcpus:     runner.spec.size.vcpus(),
			memory_mb: runner.spec.size.memory_mb(),
		}),
	};
	let created = lifecycle::create(state, &team, spec)
		.await
		.map_err(|error| create_failed(runner, &error))?;
	let node = match routes::get_route(&state.cluster.kv, &created.sandbox_id).await {
		Ok(Some(route)) => route.node_id.to_string(),
		_ => String::new(),
	};
	Ok((created.sandbox_id, node))
}

/// Provisions runner `name` under a fresh claim.
async fn provision(state: &AppState, name: &str) {
	let local = state.cluster.local_id().0;
	let runner = match state.store.claim_ci_runner(name, local, CLAIM_STALE).await {
		Ok(Some(runner)) => runner,
		Ok(None) => return,
		Err(error) => {
			tracing::warn!(%error, runner = name, "claiming a CI runner failed");
			return;
		},
	};
	let attempt = runner.attempt;
	let github = match Github::load(state).await {
		Ok(Some(github)) => github,
		Ok(None) => {
			let next = retry(&runner, "no GitHub App is configured".into(), false);
			step(state, name, attempt, &next).await;
			return;
		},
		Err(error) => {
			let next = retry(&runner, error.message, true);
			step(state, name, attempt, &next).await;
			return;
		},
	};
	tracing::info!(
		runner = name,
		attempt,
		state = runner.state.as_str(),
		"provisioning a CI runner"
	);

	// 1. The sandbox.
	let Some(sandbox_id) = ensure_sandbox(state, &runner).await else {
		return;
	};

	// 2. The JIT runner registration.
	let labels = vec![runner.spec.label.clone()];
	let mut jit = github
		.generate_jitconfig(runner.installation_id, &runner.repo, name, &labels)
		.await;
	if let Err(error) = &jit
		&& error.is(StatusCode::CONFLICT)
	{
		// An earlier claim registered the name: replace that registration.
		if let Ok(existing) = github.runners(runner.installation_id, &runner.repo).await
			&& let Some(old) = existing.iter().find(|old| old.name == name)
		{
			let _ = github
				.delete_runner(runner.installation_id, &runner.repo, old.id)
				.await;
		}
		jit = github
			.generate_jitconfig(runner.installation_id, &runner.repo, name, &labels)
			.await;
	}
	let jit = match jit {
		Ok(jit) => jit,
		Err(error) => {
			let next = github_failed(&runner, "registering the runner with GitHub", &error);
			step(state, name, attempt, &next).await;
			return;
		},
	};
	let runner_id = jit.runner.id;
	if !step(state, name, attempt, &RunnerStep::Registered { runner_id }).await {
		let _ = github
			.delete_runner(runner.installation_id, &runner.repo, runner_id)
			.await;
		return;
	}

	// 3. The runner process, behind the job gate.
	let gate = match gate_for(state, &runner).await {
		Ok(gate) => gate,
		Err(error) => {
			let _ = github
				.delete_runner(runner.installation_id, &runner.repo, runner_id)
				.await;
			step(state, name, attempt, &retry(&runner, error, false)).await;
			return;
		},
	};
	let started = start_runner(state, &sandbox_id, jit.encoded_jit_config, gate).await;
	match started {
		Ok(pid) => {
			tracing::info!(runner = name, sandbox = %sandbox_id, pid, "CI runner started");
			step(state, name, attempt, &RunnerStep::Started).await;
		},
		Err(error) => {
			// The JIT config is spent: drop the registration before a retry.
			let _ = github
				.delete_runner(runner.installation_id, &runner.repo, runner_id)
				.await;
			let next = retry(&runner, format!("starting the runner failed: {error}"), true);
			step(state, name, attempt, &next).await;
		},
	}
}

/// The sandbox of a claimed runner: its own, an earlier claim's, or a new
/// one (`None`: the step failed or the claim was lost; already recorded).
async fn ensure_sandbox(state: &AppState, runner: &CiRunnerRow) -> Option<String> {
	let (name, attempt) = (runner.name.as_str(), runner.attempt);
	if let Some(id) = &runner.sandbox_id {
		return Some(id.clone());
	}
	let found = match find_sandbox(state, name).await {
		Ok(found) => found,
		Err(error) => {
			step(state, name, attempt, &retry(runner, error.message, true)).await;
			return None;
		},
	};
	let (id, node, fresh) = match found {
		Some((id, node)) => (id, node, false),
		None => match create_sandbox(state, runner).await {
			Ok((id, node)) => (id, node, true),
			Err(next) => {
				tracing::info!(runner = name, ?next, "CI sandbox not created");
				step(state, name, attempt, &next).await;
				return None;
			},
		},
	};
	let created = RunnerStep::Created { sandbox_id: id.clone(), node_id: node };
	if !step(state, name, attempt, &created).await {
		// The claim was lost or the runner is gone. A newer claim may adopt
		// this sandbox, so it is not killed here: the orphan reaper kills it
		// once no runner record owns it.
		return None;
	}
	tracing::info!(runner = name, sandbox = %id, adopted = !fresh, "CI runner sandbox ready");
	Some(id)
}

/// The job gate of `runner` ([`gate::job_started_hook`]) from the current
/// public-repository policy of its account. Fails closed: a runner whose job
/// is unknown counts as serving a public repository, an unknown account as
/// one with public repositories off.
async fn gate_for(state: &AppState, runner: &CiRunnerRow) -> Result<String, String> {
	let private = match runner.for_job {
		Some(job) => state
			.store
			.ci_job(job)
			.await
			.map_err(|error| format!("reading the job failed: {error}"))?
			.is_some_and(|job| job.private),
		None => false,
	};
	let policy = state
		.store
		.ci_account(runner.installation_id)
		.await
		.map_err(|error| format!("reading the account failed: {error}"))?
		.map(|account| account.public)
		.unwrap_or_default();
	Ok(gate::job_started_hook(&runner.repo, private, &policy))
}

/// Starts the runner as the runner user (detached): writes the job gate
/// hook, then `run.sh --jitconfig <config>` ([`gate::start_command`]).
async fn start_runner(
	state: &AppState,
	sandbox_id: &str,
	jit: String,
	gate: String,
) -> Result<u32, String> {
	let route = routes::get_route(&state.cluster.kv, sandbox_id)
		.await
		.map_err(|error| error.message)?
		.ok_or_else(|| format!("sandbox {sandbox_id} is not running"))?;
	let client = state.node(route.node_id).map_err(|error| error.message)?;
	let request = StartProcess {
		cmd:  "/bin/sh".into(),
		args: vec!["-c".into(), gate::start_command(JIT_ENV, GATE_ENV)],
		envs: BTreeMap::from([(JIT_ENV.to_owned(), jit), (GATE_ENV.to_owned(), gate)]),
		cwd:  Some(RUNNER_DIR.into()),
		user: Some(RUNNER_USER.into()),
	};
	client
		.start_process(sandbox_id, &request)
		.await
		.map_err(|error| error.to_string())
}

// -------------------------------------------------------------- teardown

async fn kill(state: &AppState, sandbox_id: &str) -> bool {
	match lifecycle::kill(state, None, sandbox_id, KillReason::Request).await {
		Ok(()) => true,
		Err(error) if error.status == StatusCode::NOT_FOUND => true,
		Err(error) => {
			tracing::warn!(error = %error.message, sandbox = sandbox_id, "killing a CI sandbox failed");
			false
		},
	}
}

/// Why a GitHub registration was not removed.
enum Unregistered {
	/// The runner runs a job (422).
	Busy,
	/// Try again later.
	Retry(GithubError),
}

/// Removes `runner`'s GitHub registration: by id, or by name when the
/// registration's reply was lost (no id recorded). Without an app there is
/// nothing to remove; an app that failed to load (`Err`) is retried;
/// permanent GitHub errors are logged and given up.
async fn unregister(
	github: Result<Option<&Github>, &str>,
	runner: &CiRunnerRow,
) -> Result<(), Unregistered> {
	let github = match github {
		Ok(Some(github)) => github,
		Ok(None) => return Ok(()),
		Err(message) => {
			return Err(Unregistered::Retry(GithubError {
				status:       None,
				message:      format!("the GitHub App is unusable: {message}"),
				rate_limited: false,
			}));
		},
	};
	let (installation, repo) = (runner.installation_id, runner.repo.as_str());
	let id = match runner.runner_id {
		Some(id) => id,
		None => match github.runners(installation, repo).await {
			Ok(registered) => match registered.iter().find(|r| r.name == runner.name) {
				Some(found) => found.id,
				None => return Ok(()),
			},
			Err(error) if error.is_transient() => return Err(Unregistered::Retry(error)),
			Err(error) => {
				tracing::warn!(%error, runner = %runner.name, "listing GitHub runners failed");
				return Ok(());
			},
		},
	};
	match github.delete_runner(installation, repo, id).await {
		Ok(()) => Ok(()),
		Err(error) if error.is(StatusCode::UNPROCESSABLE_ENTITY) => Err(Unregistered::Busy),
		Err(error) if error.is_transient() => Err(Unregistered::Retry(error)),
		Err(error) => {
			tracing::warn!(%error, runner = %runner.name, "removing a CI runner from GitHub failed");
			Ok(())
		},
	}
}

/// Removes a stopping runner: its sandbox, its GitHub registration, its
/// record. A terminal stop (job done, hold over, time cap, sandbox gone,
/// stolen) kills the sandbox first, whatever GitHub answers; the record
/// stays until GitHub forgot the runner. Surplus and idle runners ask GitHub
/// first: a runner that picked up a job in the meantime (422) is kept.
async fn teardown(state: &AppState, github: Result<Option<&Github>, &str>, name: &str) {
	let runner = match state.store.ci_runner(name).await {
		Ok(Some(runner)) if runner.state == RunnerState::Stopping => runner,
		_ => return,
	};
	let terminal = !runner.stop.is_some_and(StopReason::yields_to_busy);
	let kill_sandbox = async || match &runner.sandbox_id {
		Some(sandbox) => kill(state, sandbox).await,
		None => true,
	};
	if terminal && !kill_sandbox().await {
		tokio::time::sleep(RETRY).await;
		return;
	}
	match unregister(github, &runner).await {
		Ok(()) => {},
		Err(Unregistered::Busy) if !terminal => {
			tracing::info!(runner = name, "CI runner picked up a job; keeping it");
			let _ = state
				.store
				.step_ci_runner(name, None, &RunnerStep::Revive)
				.await;
			return;
		},
		Err(Unregistered::Busy) => {
			// Its sandbox is gone; GitHub notices the runner is offline soon.
			tracing::info!(runner = name, "GitHub still reports the CI runner busy; retrying");
			tokio::time::sleep(RETRY).await;
			return;
		},
		Err(Unregistered::Retry(error)) => {
			tracing::warn!(%error, runner = name, "removing a CI runner from GitHub failed; retrying");
			tokio::time::sleep(RETRY).await;
			return;
		},
	}
	if !terminal && !kill_sandbox().await {
		tokio::time::sleep(RETRY).await;
		return;
	}
	match state
		.store
		.step_ci_runner(name, None, &RunnerStep::Removed)
		.await
	{
		Ok(_) => {
			tracing::info!(runner = name, stop = ?runner.stop, sandbox = ?runner.sandbox_id, "CI runner removed");
			state.ci.wake();
		},
		Err(error) => tracing::warn!(%error, runner = name, "forgetting a CI runner failed"),
	}
}

/// Keeps a held sandbox alive until its hold ends.
async fn keep_held(state: &AppState, runner: &CiRunnerRow) {
	let (Some(sandbox), Some(until)) = (&runner.sandbox_id, runner.held_until) else {
		return;
	};
	let Ok(Some(route)) = routes::get_route(&state.cluster.kv, sandbox).await else {
		return;
	};
	let until_ms = u64::try_from(until.timestamp_millis()).unwrap_or_default() + 60_000;
	if route.end_at_ms >= until_ms {
		return;
	}
	let Ok(Some(team)) = state.store.team_by_id(&runner.team_id).await else {
		return;
	};
	let remaining = until - Utc::now();
	let timeout_s = remaining.num_seconds().max(0) + 60;
	if let Err(error) = lifecycle::set_timeout(state, &team, sandbox, timeout_s).await {
		tracing::warn!(error = %error.message, sandbox = %sandbox, "extending a held CI sandbox failed");
	}
}

// ------------------------------------------------------------- reconcile

pub(super) async fn reconcile(state: &AppState) -> Result<(), ApiError> {
	let github = Github::load(state).await.ok().flatten();
	let runners: HashMap<String, CiRunnerRow> = state
		.store
		.ci_runners()
		.await?
		.into_iter()
		.map(|runner| (runner.name.clone(), runner))
		.collect();
	reap_sandboxes(state, &runners).await?;
	stop_lost_runners(state, &runners).await?;
	let Some(github) = github else {
		return Ok(());
	};
	if let Err(error) = redeliver(state, &github).await {
		tracing::warn!(%error, "redelivering CI webhooks failed");
	}
	poll_jobs(state, &github).await?;
	reap_github_runners(state, &github, &runners).await?;
	Ok(())
}

/// Kills CI sandboxes no runner record owns.
async fn reap_sandboxes(
	state: &AppState,
	runners: &HashMap<String, CiRunnerRow>,
) -> Result<(), ApiError> {
	let filter = SandboxFilter {
		states: vec![SandboxState::Running, SandboxState::Paused],
		..SandboxFilter::default()
	};
	let rows = state
		.store
		.list_sandboxes(&filter, ListOrder::Asc, None, u32::MAX)
		.await?;
	let grace = Utc::now() - chrono(ORPHAN_GRACE);
	for row in rows {
		let Some(name) = row.metadata.get(META_RUNNER) else {
			continue;
		};
		let owned = runners.get(name).is_some_and(|runner| {
			runner.sandbox_id.as_deref() == Some(row.id.as_str())
				// A claim may still be recording (or adopting) it.
				|| (runner.sandbox_id.is_none()
					&& matches!(runner.state, RunnerState::Creating | RunnerState::Pending))
		});
		if !owned && row.started_at < grace {
			tracing::info!(sandbox = %row.id, runner = %name, "killing an orphaned CI sandbox");
			kill(state, &row.id).await;
		}
	}
	Ok(())
}

/// Stops runners whose sandbox ended underneath them.
async fn stop_lost_runners(
	state: &AppState,
	runners: &HashMap<String, CiRunnerRow>,
) -> Result<(), ApiError> {
	for runner in runners.values() {
		let watched = matches!(
			runner.state,
			RunnerState::Registering | RunnerState::Idle | RunnerState::Busy | RunnerState::Held
		);
		let Some(sandbox) = runner.sandbox_id.as_deref().filter(|_| watched) else {
			continue;
		};
		let alive = state
			.store
			.sandbox(sandbox)
			.await?
			.is_some_and(|row| row.state() != SandboxState::Killed);
		if !alive {
			tracing::warn!(runner = %runner.name, sandbox, "CI runner sandbox ended");
			state
				.store
				.step_ci_runner(&runner.name, None, &RunnerStep::Stop(StopReason::Gone))
				.await?;
			state.ci.wake();
		}
	}
	Ok(())
}

/// Asks GitHub to redeliver recent failed webhook deliveries of the CI
/// events. GitHub lists failures only (`status=failure`, newest first, up to
/// [`REDELIVER_PAGES`] pages back to [`REDELIVER_WINDOW`]); deliveries vmon
/// itself answered with 2xx (`Store::ci_delivery_accepted`, e.g. a relay in
/// front of it failed afterwards) and other events of a shared app are
/// skipped. Each delivery is requested at most [`MAX_REDELIVERIES`] times.
pub(super) async fn redeliver(state: &AppState, github: &Github) -> Result<(), GithubError> {
	let cutoff = Utc::now() - REDELIVER_WINDOW;
	let deliveries = github.failed_deliveries(cutoff, REDELIVER_PAGES).await?;
	// Newest failed attempt per delivery GUID.
	let mut by_guid: HashMap<&str, &Delivery> = HashMap::new();
	for delivery in deliveries.iter().filter(|delivery| {
		matches!(
			delivery.event.as_str(),
			"workflow_job" | "installation" | "installation_repositories"
		)
	}) {
		let entry = by_guid.entry(delivery.guid.as_str()).or_insert(delivery);
		if delivery.delivered_at > entry.delivered_at {
			*entry = delivery;
		}
	}
	for (guid, delivery) in by_guid {
		match state.store.ci_delivery_accepted(guid).await {
			Ok(false) => {},
			Ok(true) => continue,
			Err(error) => {
				tracing::warn!(%error, delivery = guid, "reading CI webhook marks failed");
				continue;
			},
		}
		let requested = state
			.store
			.count_ci_redelivery(guid)
			.await
			.unwrap_or(u32::MAX);
		if requested >= MAX_REDELIVERIES {
			continue;
		}
		tracing::info!(delivery = guid, id = delivery.id, "redelivering a failed CI webhook");
		github.redeliver(delivery.id).await?;
	}
	Ok(())
}

/// Up to `n` of the sorted `items`, starting after `cursor` and wrapping
/// around, so successive calls visit every item; moves the cursor to the
/// last one returned.
pub(super) fn rotate<T: Ord + Clone>(
	items: &[T],
	cursor: &parking_lot::Mutex<Option<T>>,
	n: usize,
) -> Vec<T> {
	let mut cursor = cursor.lock();
	let start = cursor
		.as_ref()
		.map_or(0, |last| items.partition_point(|item| item <= last));
	let picked: Vec<T> = items
		.iter()
		.cycle()
		.skip(start)
		.take(n.min(items.len()))
		.cloned()
		.collect();
	if let Some(last) = picked.last() {
		*cursor = Some(last.clone());
	}
	picked
}

/// Polls unfinished jobs whose webhooks seem lost.
async fn poll_jobs(state: &AppState, github: &Github) -> Result<(), ApiError> {
	let stale = Utc::now() - chrono(POLL_AFTER);
	let mut jobs: BTreeMap<u64, CiJobRow> = state
		.store
		.ci_active_jobs()
		.await?
		.into_iter()
		.filter(|job| job.github != GithubStatus::Completed && job.queued_at < stale)
		.map(|job| (job.job_id, job))
		.collect();
	let ids: Vec<u64> = jobs.keys().copied().collect();
	for id in rotate(&ids, &state.ci.poll_cursor, POLL_BATCH) {
		let Some(job) = jobs.remove(&id) else {
			continue;
		};
		let reported = match github.job(job.installation_id, &job.repo, job.job_id).await {
			Ok(reported) => reported,
			Err(error) => {
				tracing::debug!(%error, job = job.job_id, "polling a CI job failed");
				continue;
			},
		};
		let Some(status) = super::webhook::github_status(&reported.status) else {
			continue;
		};
		let update = super::webhook::job_update(status, job, &reported);
		state.store.record_ci_job(&update).await?;
	}
	Ok(())
}

/// Removes offline `vmon-` GitHub runners no runner record owns.
async fn reap_github_runners(
	state: &AppState,
	github: &Github,
	runners: &HashMap<String, CiRunnerRow>,
) -> Result<(), ApiError> {
	let mut repos: BTreeSet<(u64, String)> = runners
		.values()
		.map(|runner| (runner.installation_id, runner.repo.clone()))
		.collect();
	repos.extend(
		state
			.store
			.ci_active_jobs()
			.await?
			.into_iter()
			.map(|job| (job.installation_id, job.repo)),
	);
	let repos: Vec<(u64, String)> = repos.into_iter().collect();
	for (installation, repo) in rotate(&repos, &state.ci.repo_cursor, RUNNER_REPOS) {
		let Ok(registered) = github.runners(installation, &repo).await else {
			continue;
		};
		for runner in registered {
			let candidate = runner.name.starts_with("vmon-")
				&& !runners.contains_key(&runner.name)
				&& runner.status == "offline"
				&& !runner.busy;
			// A runner recorded since the reconcile started is not an orphan.
			if candidate && state.store.ci_runner(&runner.name).await?.is_none() {
				tracing::info!(runner = %runner.name, %repo, "removing an orphaned GitHub runner");
				let _ = github.delete_runner(installation, &repo, runner.id).await;
			}
		}
	}
	Ok(())
}
