//! The driver side of the CI cache (ARCHITECTURE.md §9.7): each job's cache
//! scope from trusted GitHub data, the per-sandbox grants the nodes serve
//! by, and the leader's index sweep.
//!
//! A job's scope follows `GITHUB_REF`: it writes to its own ref and reads its
//! own ref, then (pull requests) the base branch, then the default branch.
//! It is resolved from the job's workflow run as GitHub reports it to the
//! app, never from anything the job sends. Every pass the controller writes
//! `db/ci_cache_grant/<sandbox>` for each runner sandbox (its runner, the job
//! GitHub reported on it, that job's scope) and deletes the grants of
//! sandboxes no runner owns.

use std::{
	collections::{BTreeMap, BTreeSet},
	sync::Arc,
	time::{Duration, Instant},
};

use chrono::Utc;
use vmon_node::ci_cache::{
	index,
	records::{CacheGrant, CacheScope},
};

use super::{
	controller::{RETRY, SANDBOX_TIMEOUT, spawn_once},
	github::{Github, PullRequest, Run},
};
use crate::{
	lifecycle::LeaderTicks,
	state::AppState,
	store::ci::{CiJobRow, CiRunnerRow, JobState},
};

/// Lifetime of a grant: the sandbox's (a grant outliving a held sandbox's
/// timeout is rewritten by the next pass).
const GRANT_TTL: Duration = SANDBOX_TIMEOUT.saturating_add(Duration::from_mins(10));
/// Period of the index sweep (expired entries, repository budgets).
const SWEEP_EVERY: Duration = Duration::from_mins(10);

// ------------------------------------------------------------------ scope

fn branch_ref(branch: &str) -> String {
	format!("refs/heads/{branch}")
}

/// Whether runs of `event` run for a pull request (`GITHUB_REF` is the
/// pull request's merge ref, except for `pull_request_target`).
fn pull_request_event(event: &str) -> bool {
	event.starts_with("pull_request")
}

/// The pull request of `run` among `pulls`: the one whose head is the run's
/// head commit (for `pull_request_target`, which runs on the base branch's
/// commit, also the one whose base is).
pub fn pull_of<'p>(run: &Run, pulls: &'p [PullRequest]) -> Option<&'p PullRequest> {
	let target = run.event == "pull_request_target";
	pulls.iter().find(|pull| {
		!run.head_sha.is_empty()
			&& (pull.head.sha == run.head_sha || (target && pull.base.sha == run.head_sha))
	})
}

/// The pull request of `run` among those GitHub lists on it ([`pull_of`];
/// no fallback to a listed one that does not match: a fork's run must not
/// pass for another pull request).
pub fn run_pull(run: &Run) -> Option<&PullRequest> {
	pull_of(run, &run.pull_requests)
}

/// The cache scope of a job of workflow run `run_id` (`run`, its pull
/// request `pull`) in repository `repo_id`/`repo` whose default branch is
/// `default_branch`, following `GITHUB_REF`:
/// - `pull_request*` (not `_target`): writes `refs/pull/<n>/merge`, reads it,
///   the base branch, the default branch;
/// - `pull_request_target`: runs in the base branch's context: writes and reads
///   the base branch, then the default branch;
/// - `release`: writes the tag `refs/tags/<head branch>`;
/// - anything else (`push`, `workflow_dispatch`, `schedule`, `merge_group`, …):
///   writes `refs/heads/<head branch>`;
///
/// each reading its own ref, then the default branch. A pull request run
/// whose pull request is unknown, or a run without a head branch, writes an
/// isolated `refs/vmon/runs/<run id>` and reads it and the default branch.
pub fn scope_of(
	run_id: u64,
	run: &Run,
	pull: Option<&PullRequest>,
	default_branch: &str,
	repo_id: u64,
	repo: &str,
) -> CacheScope {
	let head = run
		.head_branch
		.as_deref()
		.filter(|branch| !branch.is_empty());
	let base = pull.map(|pull| branch_ref(&pull.base.branch));
	let (write, base) = match run.event.as_str() {
		"pull_request_target" => (base, None),
		event if pull_request_event(event) => match pull {
			Some(pull) => (Some(format!("refs/pull/{}/merge", pull.number)), base),
			None => (None, None),
		},
		"release" => (head.map(|tag| format!("refs/tags/{tag}")), None),
		_ => (head.map(branch_ref), None),
	};
	let write = write.unwrap_or_else(|| format!("refs/vmon/runs/{run_id}"));
	let mut read = vec![write.clone()];
	let default = (!default_branch.is_empty()).then(|| branch_ref(default_branch));
	for scope in [base, default].into_iter().flatten() {
		if !read.contains(&scope) {
			read.push(scope);
		}
	}
	CacheScope { repo_id, repo: repo.to_owned(), write, read }
}

/// Resolves `job`'s scope from GitHub: its workflow run, the repository (id
/// and default branch, when the job record lacks them), and for pull request
/// runs from forks (GitHub lists no pull request on those runs) the pull
/// request whose head is the run's head commit.
async fn resolve(github: &Github, job: &CiJobRow) -> Result<CacheScope, String> {
	let installation = job.installation_id;
	let run = github
		.run(installation, &job.repo, job.run_id)
		.await
		.map_err(|error| format!("reading run {}: {error}", job.run_id))?;
	let (repo_id, default_branch) =
		if let (Some(id), Some(branch)) = (job.repo_id, &job.default_branch) {
			(id, branch.clone())
		} else {
			let repository = github
				.repository(installation, &job.repo)
				.await
				.map_err(|error| format!("reading {}: {error}", job.repo))?;
			let id = repository
				.id
				.ok_or_else(|| format!("GitHub reported no id for {}", job.repo))?;
			(id, repository.default_branch.unwrap_or_default())
		};
	let mut pull = run_pull(&run).cloned();
	let fork = run
		.head_repository
		.as_ref()
		.map(|repository| repository.owner.login.as_str());
	if pull.is_none()
		&& pull_request_event(&run.event)
		&& let (Some(owner), Some(branch)) = (fork, run.head_branch.as_deref())
	{
		let pulls = github
			.pulls_by_head(installation, &job.repo, owner, branch)
			.await
			.map_err(|error| format!("finding the pull request of {owner}:{branch}: {error}"))?;
		pull = pull_of(&run, &pulls).cloned();
	}
	Ok(scope_of(job.run_id, &run, pull.as_ref(), &default_branch, repo_id, &job.repo))
}

// ----------------------------------------------------------------- grants

/// The grant each runner sandbox should have, by sandbox id: its runner, the
/// job GitHub reported running on it, and that job's scope once resolved (a
/// refused job gets none).
pub fn desired_grants(
	runners: &[CiRunnerRow],
	jobs: &BTreeMap<u64, &CiJobRow>,
) -> BTreeMap<String, CacheGrant> {
	runners
		.iter()
		.filter_map(|runner| {
			let sandbox = runner.sandbox_id.clone()?;
			let scope = runner
				.job_id
				.and_then(|id| jobs.get(&id))
				.filter(|job| job.state != JobState::Refused)
				.and_then(|job| job.cache_scope.clone());
			Some((sandbox, CacheGrant { runner: runner.name.clone(), job_id: runner.job_id, scope }))
		})
		.collect()
}

/// What turns `current` into `desired`: grants to write (new or changed) and
/// sandboxes whose grant goes.
pub fn grant_changes(
	desired: BTreeMap<String, CacheGrant>,
	current: &BTreeMap<String, CacheGrant>,
) -> (Vec<(String, CacheGrant)>, Vec<String>) {
	let deletes = current
		.keys()
		.filter(|sandbox| !desired.contains_key(*sandbox))
		.cloned()
		.collect();
	let puts = desired
		.into_iter()
		.filter(|(sandbox, grant)| current.get(sandbox) != Some(grant))
		.collect();
	(puts, deletes)
}

/// Brings the grants in line with the runner records (every controller
/// pass) and resolves the scopes of the runners' jobs that lack one. Errors
/// are logged; the next pass retries.
pub(super) async fn sync_grants(
	state: &AppState,
	github: Option<&Arc<Github>>,
	runners: &[CiRunnerRow],
	active: &[CiJobRow],
) {
	if let Err(error) = sync(state, github, runners, active).await {
		tracing::warn!(error = format!("{error:#}"), "syncing CI cache grants failed");
	}
}

async fn sync(
	state: &AppState,
	github: Option<&Arc<Github>>,
	runners: &[CiRunnerRow],
	active: &[CiJobRow],
) -> anyhow::Result<()> {
	// Finished jobs whose runner still has its sandbox (stopping, held).
	let mut finished = Vec::new();
	for id in runners
		.iter()
		.filter(|runner| runner.sandbox_id.is_some())
		.filter_map(|runner| runner.job_id)
	{
		if !active.iter().any(|job| job.job_id == id)
			&& let Some(job) = state.store.ci_job(id).await?
		{
			finished.push(job);
		}
	}
	let jobs: BTreeMap<u64, &CiJobRow> = active
		.iter()
		.chain(&finished)
		.map(|job| (job.job_id, job))
		.collect();
	if let Some(github) = github {
		resolve_scopes(state, github, runners, &jobs);
	}
	let kv = &state.cluster.kv;
	let current = index::grants(kv).await?;
	let (puts, deletes) = grant_changes(desired_grants(runners, &jobs), &current);
	for (sandbox, grant) in puts {
		index::put_grant(kv, &sandbox, &grant, GRANT_TTL).await?;
	}
	for sandbox in deletes {
		index::delete_grant(kv, &sandbox).await?;
	}
	Ok(())
}

/// Starts scope resolutions for the runners' jobs (the job GitHub reported,
/// or ahead of it the job the runner was provisioned for) that lack a scope;
/// a failed one is retried after [`RETRY`].
fn resolve_scopes(
	state: &AppState,
	github: &Arc<Github>,
	runners: &[CiRunnerRow],
	jobs: &BTreeMap<u64, &CiJobRow>,
) {
	let wanted: BTreeSet<u64> = runners
		.iter()
		.filter_map(|runner| runner.job_id.or(runner.for_job))
		.filter(|id| {
			jobs
				.get(id)
				.is_some_and(|job| job.cache_scope.is_none() && job.state != JobState::Refused)
		})
		.collect();
	let now = Instant::now();
	let due: Vec<u64> = {
		let mut retry = state.ci.scope_retry.lock();
		retry.retain(|id, _| wanted.contains(id));
		wanted
			.into_iter()
			.filter(|id| retry.get(id).is_none_or(|at| *at <= now))
			.collect()
	};
	for id in due {
		let (task_state, github, job) = (state.clone(), Arc::clone(github), jobs[&id].clone());
		spawn_once(state, format!("cache:{id}"), async move {
			let resolved = match resolve(&github, &job).await {
				Ok(scope) => task_state
					.store
					.set_ci_job_cache_scope(id, &scope)
					.await
					.map(drop)
					.map_err(|error| error.to_string()),
				Err(error) => Err(error),
			};
			match resolved {
				Ok(()) => {
					task_state.ci.scope_retry.lock().remove(&id);
					task_state.ci.wake();
				},
				Err(error) => {
					tracing::warn!(job = id, %error, "resolving a CI job's cache scope failed");
					task_state
						.ci
						.scope_retry
						.lock()
						.insert(id, Instant::now() + RETRY);
				},
			}
		});
	}
}

// ------------------------------------------------------------------ sweep

/// Leader-only loop removing expired cache entries and enforcing the
/// per-repository budget every [`SWEEP_EVERY`].
pub(super) async fn run_sweeper(state: AppState) {
	let mut ticks = LeaderTicks::new(&state, SWEEP_EVERY);
	while let Some(leader) = ticks.tick().await {
		if !leader {
			continue;
		}
		match index::sweep(&state.cluster.kv, Utc::now(), index::REPO_BUDGET).await {
			Ok(swept) if swept != index::Swept::default() => tracing::info!(
				expired = swept.expired,
				evicted = swept.evicted,
				bytes = swept.bytes,
				"CI cache sweep removed entries"
			),
			Ok(_) => {},
			Err(error) => tracing::warn!(error = format!("{error:#}"), "CI cache sweep failed"),
		}
	}
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::*;
	use crate::{
		ci::labels::{RunnerSpec, Size},
		store::{
			self,
			ci::{GithubStatus, RunnerState},
		},
	};

	fn run(event: &str, head_branch: Option<&str>, pulls: &serde_json::Value) -> Run {
		serde_json::from_value(json!({
			"actor": { "login": "alice" },
			"triggering_actor": null,
			"event": event,
			"head_branch": head_branch,
			"head_sha": "c0ffee",
			"pull_requests": pulls,
			"head_repository": { "owner": { "login": "fork-owner" } },
		}))
		.unwrap()
	}

	fn pulls(number: u64, base: &str, sha: &str) -> serde_json::Value {
		json!([{ "number": number, "base": { "ref": base, "sha": "b" },
			"head": { "ref": "feature", "sha": sha } }])
	}

	fn scope(run: &Run, default: &str) -> CacheScope {
		scope_of(9, run, run_pull(run), default, 77, "acme/app")
	}

	#[test]
	fn pull_requests_write_their_merge_ref_and_read_base_then_default() {
		let pr = run("pull_request", Some("feature"), &pulls(5, "release/1", "c0ffee"));
		let got = scope(&pr, "main");
		assert_eq!((got.repo_id, got.repo.as_str()), (77, "acme/app"));
		assert_eq!(got.write, "refs/pull/5/merge");
		assert_eq!(got.read, ["refs/pull/5/merge", "refs/heads/release/1", "refs/heads/main"]);
		// Base = default: listed once.
		let to_main = run("pull_request", Some("feature"), &pulls(6, "main", "c0ffee"));
		assert_eq!(scope(&to_main, "main").read, ["refs/pull/6/merge", "refs/heads/main"]);
		// Review events too; never the head branch (a fork may name it `main`).
		let review = run("pull_request_review", Some("main"), &pulls(7, "dev", "c0ffee"));
		let got = scope(&review, "main");
		assert_eq!(got.write, "refs/pull/7/merge");
		assert!(!got.read.contains(&"refs/heads/feature".to_owned()));
	}

	#[test]
	fn pushes_read_their_branch_and_the_default_only() {
		let push = run("push", Some("dev"), &json!([]));
		let got = scope(&push, "main");
		assert_eq!(got.write, "refs/heads/dev");
		assert_eq!(got.read, ["refs/heads/dev", "refs/heads/main"]);
		let main = run("push", Some("main"), &json!([]));
		assert_eq!(scope(&main, "main").read, ["refs/heads/main"]);
		let dispatch = run("workflow_dispatch", Some("dev"), &pulls(5, "main", "c0ffee"));
		assert_eq!(scope(&dispatch, "main").write, "refs/heads/dev", "only PR events use a PR");
		let release = run("release", Some("v1.2"), &json!([]));
		assert_eq!(scope(&release, "main").read, ["refs/tags/v1.2", "refs/heads/main"]);
	}

	#[test]
	fn pull_request_target_runs_in_the_base_context() {
		let target = run("pull_request_target", Some("feature"), &pulls(5, "dev", "c0ffee"));
		let got = scope(&target, "main");
		assert_eq!(got.write, "refs/heads/dev");
		assert_eq!(got.read, ["refs/heads/dev", "refs/heads/main"]);
		// It runs on the base's commit: found by base among several.
		let two = json!([
			{ "number": 5, "base": { "ref": "dev", "sha": "x" }, "head": { "ref": "f", "sha": "y" } },
			{ "number": 6, "base": { "ref": "rel", "sha": "c0ffee" },
				"head": { "ref": "f", "sha": "z" } },
		]);
		let target = run("pull_request_target", Some("f"), &two);
		assert_eq!(scope(&target, "main").write, "refs/heads/rel");
	}

	#[test]
	fn fork_pull_requests_are_found_by_head_commit() {
		// GitHub lists no pull request on a fork's run; the pulls lookup by
		// head finds candidates, the head commit picks one.
		let fork = run("pull_request", Some("main"), &json!([]));
		assert!(pull_of(&fork, &fork.pull_requests).is_none());
		let found: Vec<PullRequest> = serde_json::from_value(json!([
			{ "number": 3, "base": { "ref": "main" }, "head": { "ref": "main", "sha": "old" } },
			{ "number": 4, "base": { "ref": "main" }, "head": { "ref": "main", "sha": "c0ffee" } },
		]))
		.unwrap();
		let pull = pull_of(&fork, &found);
		assert_eq!(pull.map(|pull| pull.number), Some(4));
		let got = scope_of(9, &fork, pull, "main", 77, "acme/app");
		assert_eq!(got.read, ["refs/pull/4/merge", "refs/heads/main"]);
	}

	#[test]
	fn unknown_refs_get_an_isolated_scope() {
		// Only the head commit decides, also for a single listed one.
		let moved = run("pull_request", Some("feature"), &pulls(5, "dev", "newer"));
		assert_eq!(scope(&moved, "main").write, "refs/vmon/runs/9");
		let two = json!([
			{ "number": 5, "base": { "ref": "dev" }, "head": { "ref": "f", "sha": "a" } },
			{ "number": 6, "base": { "ref": "rel" }, "head": { "ref": "f", "sha": "b" } },
		]);
		let unknown_pr = run("pull_request", Some("main"), &two);
		let got = scope(&unknown_pr, "main");
		assert_eq!(got.write, "refs/vmon/runs/9");
		assert_eq!(got.read, ["refs/vmon/runs/9", "refs/heads/main"]);
		let target = run("pull_request_target", Some("main"), &json!([]));
		assert_eq!(scope(&target, "main").write, "refs/vmon/runs/9");
		let headless = run("schedule", None, &json!([]));
		assert_eq!(scope(&headless, "main").write, "refs/vmon/runs/9");
		assert_eq!(scope(&headless, "").read, ["refs/vmon/runs/9"], "no default branch known");
		// A run as old records parse it (no event, no pull requests).
		let bare: Run = serde_json::from_value(json!({ "actor": { "login": "a" } })).unwrap();
		assert_eq!(scope(&bare, "main").write, "refs/vmon/runs/9");
	}

	fn runner(name: &str, sandbox: Option<&str>, job_id: Option<u64>) -> CiRunnerRow {
		let now = store::now();
		CiRunnerRow {
			name: name.into(),
			installation_id: 1,
			account: "acme".into(),
			repo: "acme/app".into(),
			team_id: "team".into(),
			spec: RunnerSpec {
				label:    "vmon".into(),
				size:     Size::Medium,
				template: None,
				debug:    false,
			},
			for_job: job_id,
			job_id,
			state: RunnerState::Busy,
			stop: None,
			attempt: 1,
			claimed_by: None,
			claimed_at: None,
			failures: 0,
			reason: None,
			next_try_at: None,
			sandbox_id: sandbox.map(str::to_owned),
			node_id: None,
			runner_id: None,
			created_at: now,
			idle_since: None,
			held_until: None,
			updated_at: now,
		}
	}

	fn job(id: u64, state: JobState, scope: Option<CacheScope>) -> CiJobRow {
		let now = store::now();
		CiJobRow {
			job_id: id,
			run_id: 9,
			run_attempt: 1,
			installation_id: 1,
			account: "acme".into(),
			repo: "acme/app".into(),
			private: true,
			repo_id: Some(77),
			default_branch: Some("main".into()),
			cache_scope: scope,
			workflow: "CI".into(),
			job: "test".into(),
			branch: Some("main".into()),
			sha: "c0ffee".into(),
			html_url: String::new(),
			labels: vec!["vmon".into()],
			spec: None,
			github: GithubStatus::InProgress,
			state,
			conclusion: None,
			reason: None,
			decided: true,
			team_id: None,
			runner: None,
			provisions: 1,
			runner_name: None,
			sandbox_id: None,
			node_id: None,
			queued_at: now,
			started_at: None,
			completed_at: None,
			held_until: None,
			updated_at: now,
		}
	}

	#[test]
	fn grants_follow_runner_sandboxes_and_their_jobs_scopes() {
		let main = scope(&run("push", Some("main"), &json!([])), "main");
		let jobs = [
			job(1, JobState::Running, Some(main.clone())),
			job(2, JobState::Running, None),
			job(3, JobState::Refused, Some(main.clone())),
		];
		let by_id: BTreeMap<u64, &CiJobRow> = jobs.iter().map(|job| (job.job_id, job)).collect();
		let runners = [
			runner("vmon-1-1", Some("sb-1"), Some(1)),
			runner("vmon-2-1", Some("sb-2"), Some(2)),
			runner("vmon-3-1", Some("sb-3"), Some(3)),
			runner("vmon-4-1", Some("sb-4"), None),
			runner("vmon-5-1", None, Some(1)),
		];
		let desired = desired_grants(&runners, &by_id);
		assert_eq!(desired.keys().collect::<Vec<_>>(), ["sb-1", "sb-2", "sb-3", "sb-4"]);
		assert_eq!(desired["sb-1"], CacheGrant {
			runner: "vmon-1-1".into(),
			job_id: Some(1),
			scope:  Some(main),
		});
		assert_eq!(desired["sb-2"].scope, None, "not resolved yet");
		assert_eq!(desired["sb-3"].scope, None, "refused jobs get no scope");
		assert_eq!((desired["sb-4"].job_id, &desired["sb-4"].scope), (None, &None));

		let mut current = desired.clone();
		current.remove("sb-2");
		current.insert("sb-gone".into(), desired["sb-1"].clone());
		let mut stale = desired["sb-4"].clone();
		stale.job_id = Some(8);
		current.insert("sb-4".into(), stale);
		let (puts, deletes) = grant_changes(desired.clone(), &current);
		let put: Vec<&str> = puts.iter().map(|(sandbox, _)| sandbox.as_str()).collect();
		assert_eq!(put, ["sb-2", "sb-4"], "only new and changed grants are written");
		assert_eq!(deletes, ["sb-gone"]);
		let (puts, deletes) = grant_changes(desired.clone(), &desired);
		assert!(puts.is_empty() && deletes.is_empty());
	}
}
