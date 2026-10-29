//! CI job and runner transitions on a one-voter store: GitHub statuses only
//! move forward, decisions provision exactly one runner, runners picked up
//! by another job hand their job over, and runner steps commit only under
//! their claim.

use super::*;
use crate::{ci::labels::Size, store::testing::TestStore};

fn spec(debug: bool) -> RunnerSpec {
	RunnerSpec { label: "vmon".into(), size: Size::Medium, template: None, debug }
}

fn job(id: u64, debug: bool) -> CiJobRow {
	let now = now();
	CiJobRow {
		job_id:          id,
		run_id:          7,
		run_attempt:     1,
		installation_id: 1,
		account:         "acme".into(),
		repo:            "acme/app".into(),
		private:         true,
		workflow:        "CI".into(),
		job:             format!("job {id}"),
		branch:          Some("main".into()),
		sha:             "abc".into(),
		html_url:        String::new(),
		labels:          vec!["vmon".into()],
		spec:            Some(spec(debug)),
		github:          GithubStatus::Queued,
		state:           JobState::Queued,
		conclusion:      None,
		reason:          None,
		decided:         false,
		team_id:         None,
		runner:          None,
		provisions:      0,
		runner_name:     None,
		sandbox_id:      None,
		node_id:         None,
		queued_at:       now,
		started_at:      None,
		completed_at:    None,
		held_until:      None,
		updated_at:      now,
	}
}

fn update(status: GithubStatus, row: CiJobRow, runner: Option<&str>) -> JobUpdate {
	let conclusion = (status == GithubStatus::Completed).then(|| "success".to_owned());
	JobUpdate {
		status,
		job: row,
		runner_name: runner.map(str::to_owned),
		conclusion,
		started_at: None,
		completed_at: None,
	}
}

fn provision() -> Decision {
	Decision::Provision { team_id: "team".into() }
}

async fn queue(store: &Store, id: u64, debug: bool) -> String {
	assert_eq!(
		store
			.record_ci_job(&update(GithubStatus::Queued, job(id, debug), None))
			.await
			.unwrap(),
		Recorded::New
	);
	let decided = store
		.decide_ci_job(id, &provision())
		.await
		.unwrap()
		.unwrap();
	decided.runner.unwrap()
}

#[tokio::test]
async fn statuses_only_move_forward_and_decisions_provision_once() {
	let db = TestStore::start().await;
	let store = &db.store;
	let runner = queue(store, 10, false).await;
	assert_eq!(runner, "vmon-10-1");
	// A redelivered `queued` changes nothing; a second decision finds the job
	// handled.
	let again = update(GithubStatus::Queued, job(10, false), None);
	assert_eq!(store.record_ci_job(&again).await.unwrap(), Recorded::Duplicate);
	assert!(
		store
			.decide_ci_job(10, &provision())
			.await
			.unwrap()
			.is_none()
	);
	assert_eq!(store.ci_runners().await.unwrap().len(), 1);
	assert_eq!(store.ci_active_jobs().await.unwrap().len(), 1);

	let busy = update(GithubStatus::InProgress, job(10, false), Some(&runner));
	assert_eq!(store.record_ci_job(&busy).await.unwrap(), Recorded::Updated);
	assert_eq!(store.record_ci_job(&busy).await.unwrap(), Recorded::Duplicate);
	assert_eq!(store.record_ci_job(&again).await.unwrap(), Recorded::Duplicate);
	let row = store.ci_job(10).await.unwrap().unwrap();
	assert_eq!((row.state, row.runner.as_deref()), (JobState::Running, Some(runner.as_str())));
	let busy_runner = store.ci_runner(&runner).await.unwrap().unwrap();
	assert_eq!((busy_runner.state, busy_runner.job_id), (RunnerState::Busy, Some(10)));

	let done = update(GithubStatus::Completed, job(10, false), Some(&runner));
	assert_eq!(store.record_ci_job(&done).await.unwrap(), Recorded::Updated);
	let row = store.ci_job(10).await.unwrap().unwrap();
	assert_eq!((row.state, row.conclusion.as_deref()), (JobState::Completed, Some("success")));
	assert!(store.ci_active_jobs().await.unwrap().is_empty());
	let stopping = store.ci_runner(&runner).await.unwrap().unwrap();
	assert_eq!(
		(stopping.state, stopping.stop),
		(RunnerState::Stopping, Some(StopReason::Completed))
	);
	let history: Vec<JobState> = store
		.ci_job_events(10)
		.await
		.unwrap()
		.into_iter()
		.map(|event| event.value.state)
		.collect();
	assert_eq!(history.first(), Some(&JobState::Queued));
	assert_eq!(history.last(), Some(&JobState::Completed));
	db.finish().await;
}

#[tokio::test]
async fn a_runner_taken_by_another_job_hands_that_jobs_runner_over() {
	let db = TestStore::start().await;
	let store = &db.store;
	let a = queue(store, 1, false).await;
	let b = queue(store, 2, false).await;
	// Job 2 runs on the runner provisioned for job 1.
	let taken = update(GithubStatus::InProgress, job(2, false), Some(&a));
	store.record_ci_job(&taken).await.unwrap();
	let job1 = store.ci_job(1).await.unwrap().unwrap();
	let job2 = store.ci_job(2).await.unwrap().unwrap();
	assert_eq!(job2.runner.as_deref(), Some(a.as_str()));
	assert_eq!(job1.runner.as_deref(), Some(b.as_str()));
	assert_eq!(store.ci_runner(&b).await.unwrap().unwrap().for_job, Some(1));
	assert_eq!(store.ci_runners().await.unwrap().len(), 2);

	// Job 3's `queued` webhook was lost: it is first seen running on job 1's
	// new runner, recorded from that report and adopted; job 1 waits for a
	// fresh runner.
	store
		.record_ci_job(&update(GithubStatus::InProgress, job(3, false), Some(&b)))
		.await
		.unwrap();
	let job3 = store.ci_job(3).await.unwrap().unwrap();
	assert_eq!((job3.state, job3.runner.as_deref()), (JobState::Running, Some(b.as_str())));
	assert_eq!(job3.team_id.as_deref(), Some("team"));
	let adopted = store.ci_runner(&b).await.unwrap().unwrap();
	assert_eq!((adopted.state, adopted.job_id), (RunnerState::Busy, Some(3)));
	let job1 = store.ci_job(1).await.unwrap().unwrap();
	assert_eq!((job1.state, job1.runner.as_deref()), (JobState::Queued, None));
	let c = store
		.decide_ci_job(1, &provision())
		.await
		.unwrap()
		.unwrap()
		.runner
		.unwrap();
	assert_eq!(c, "vmon-1-2");

	// Job 4 was refused yet starts on job 1's new runner: the runner is
	// stopped as stolen and job 1 waits for yet another one.
	store
		.record_ci_job(&update(GithubStatus::Queued, job(4, false), None))
		.await
		.unwrap();
	let refused = Decision::Refuse("not allowed".into());
	store.decide_ci_job(4, &refused).await.unwrap().unwrap();
	store
		.record_ci_job(&update(GithubStatus::InProgress, job(4, false), Some(&c)))
		.await
		.unwrap();
	let job4 = store.ci_job(4).await.unwrap().unwrap();
	assert_eq!((job4.state, job4.runner.as_deref()), (JobState::Refused, None));
	assert!(job4.reason.as_deref().unwrap().contains(&c), "{:?}", job4.reason);
	let stolen = store.ci_runner(&c).await.unwrap().unwrap();
	assert_eq!((stolen.state, stolen.stop), (RunnerState::Stopping, Some(StopReason::Stolen)));
	let job1 = store.ci_job(1).await.unwrap().unwrap();
	assert_eq!((job1.state, job1.runner.as_deref()), (JobState::Queued, None));
	// Its completion leaves the refusal and the stop reason alone.
	store
		.record_ci_job(&update(GithubStatus::Completed, job(4, false), Some(&c)))
		.await
		.unwrap();
	assert_eq!(store.ci_job(4).await.unwrap().unwrap().state, JobState::Refused);
	assert_eq!(store.ci_runner(&c).await.unwrap().unwrap().stop, Some(StopReason::Stolen));
	let fresh = store.decide_ci_job(1, &provision()).await.unwrap().unwrap();
	assert_eq!(fresh.runner.as_deref(), Some("vmon-1-3"));
	db.finish().await;
}

#[tokio::test]
async fn completions_hold_debug_failures_and_release_unused_runners() {
	let db = TestStore::start().await;
	let store = &db.store;
	let held = queue(store, 1, true).await;
	store
		.record_ci_job(&update(GithubStatus::InProgress, job(1, true), Some(&held)))
		.await
		.unwrap();
	let mut failed = update(GithubStatus::Completed, job(1, true), Some(&held));
	failed.conclusion = Some("failure".into());
	store.record_ci_job(&failed).await.unwrap();
	let job1 = store.ci_job(1).await.unwrap().unwrap();
	assert_eq!(job1.state, JobState::Held);
	assert!(job1.held_until.is_some());
	assert_eq!(store.ci_runner(&held).await.unwrap().unwrap().state, RunnerState::Held);
	// The hold ends: the job completes when the runner is removed.
	store
		.step_ci_runner(&held, None, &RunnerStep::Stop(StopReason::Released))
		.await
		.unwrap();
	store
		.step_ci_runner(&held, None, &RunnerStep::Removed)
		.await
		.unwrap();
	assert_eq!(store.ci_job(1).await.unwrap().unwrap().state, JobState::Completed);
	assert!(store.ci_runner(&held).await.unwrap().is_none());

	// Cancelled before any runner picked it: its runner is surplus.
	let unused = queue(store, 2, false).await;
	let mut cancelled = update(GithubStatus::Completed, job(2, false), None);
	cancelled.conclusion = Some("cancelled".into());
	store.record_ci_job(&cancelled).await.unwrap();
	let job2 = store.ci_job(2).await.unwrap().unwrap();
	assert_eq!((job2.state, job2.started_at), (JobState::Completed, None));
	let surplus = store.ci_runner(&unused).await.unwrap().unwrap();
	assert_eq!((surplus.state, surplus.stop), (RunnerState::Stopping, Some(StopReason::Surplus)));
	db.finish().await;
}

#[tokio::test]
async fn runner_steps_commit_only_under_their_claim() {
	let db = TestStore::start().await;
	let store = &db.store;
	let name = queue(store, 5, false).await;
	let stale = Duration::from_mins(10);
	let claimed = store
		.claim_ci_runner(&name, 1, stale)
		.await
		.unwrap()
		.unwrap();
	assert_eq!((claimed.state, claimed.attempt), (RunnerState::Creating, 1));
	// A live claim is not taken over.
	assert!(
		store
			.claim_ci_runner(&name, 2, stale)
			.await
			.unwrap()
			.is_none()
	);
	// A stale claim is: the old one can no longer record its sandbox.
	let taken = store
		.claim_ci_runner(&name, 2, Duration::ZERO)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(taken.attempt, 2);
	let created = RunnerStep::Created { sandbox_id: "isbx".into(), node_id: "n1".into() };
	assert!(
		store
			.step_ci_runner(&name, Some(1), &created)
			.await
			.unwrap()
			.is_none()
	);
	let runner = store
		.step_ci_runner(&name, Some(2), &created)
		.await
		.unwrap()
		.unwrap();
	assert_eq!(runner.state, RunnerState::Registering);
	let job5 = store.ci_job(5).await.unwrap().unwrap();
	assert_eq!((job5.state, job5.sandbox_id.as_deref()), (JobState::Starting, Some("isbx")));

	// A rate limit makes the job wait with the reason.
	let retry = RunnerStep::Retry {
		reason:   "GitHub rate limit".into(),
		retry_at: now() + chrono::Duration::seconds(15),
		failure:  false,
	};
	store.step_ci_runner(&name, Some(2), &retry).await.unwrap();
	let job5 = store.ci_job(5).await.unwrap().unwrap();
	assert_eq!((job5.state, job5.reason.as_deref()), (JobState::Waiting, Some("GitHub rate limit")));
	assert!(
		store
			.claim_ci_runner(&name, 1, stale)
			.await
			.unwrap()
			.is_none(),
		"not due yet"
	);

	// The sandbox is lost while registering: the job gets a new runner.
	store
		.step_ci_runner(&name, None, &RunnerStep::Stop(StopReason::Gone))
		.await
		.unwrap();
	store
		.step_ci_runner(&name, None, &RunnerStep::Removed)
		.await
		.unwrap();
	let job5 = store.ci_job(5).await.unwrap().unwrap();
	assert_eq!((job5.state, job5.runner.as_deref()), (JobState::Queued, None));
	assert_eq!(
		store
			.decide_ci_job(5, &provision())
			.await
			.unwrap()
			.unwrap()
			.runner
			.as_deref(),
		Some("vmon-5-2")
	);

	// Giving up fails the job.
	let abandon = RunnerStep::Abandon { refuse: true, reason: "template 'x' not found".into() };
	store
		.step_ci_runner("vmon-5-2", None, &abandon)
		.await
		.unwrap();
	let job5 = store.ci_job(5).await.unwrap().unwrap();
	assert_eq!(
		(job5.state, job5.reason.as_deref()),
		(JobState::Refused, Some("template 'x' not found"))
	);
	db.finish().await;
}

#[tokio::test]
async fn terminal_stops_win_over_surplus_and_idle_ones() {
	let db = TestStore::start().await;
	let store = &db.store;
	let name = queue(store, 7, false).await;
	let idle = RunnerStep::Stop(StopReason::Idle);
	store.step_ci_runner(&name, None, &idle).await.unwrap();
	// A refusal or completion recorded while teardown waits for GitHub: the
	// teardown's 422 must not revive the runner.
	store
		.step_ci_runner(&name, None, &RunnerStep::Stop(StopReason::Stolen))
		.await
		.unwrap();
	store
		.step_ci_runner(&name, None, &RunnerStep::Revive)
		.await
		.unwrap();
	let runner = store.ci_runner(&name).await.unwrap().unwrap();
	assert_eq!((runner.state, runner.stop), (RunnerState::Stopping, Some(StopReason::Stolen)));
	// A terminal stop is never replaced by a non-terminal one.
	store.step_ci_runner(&name, None, &idle).await.unwrap();
	let runner = store.ci_runner(&name).await.unwrap().unwrap();
	assert_eq!(runner.stop, Some(StopReason::Stolen));

	// An idle stop still yields to a busy runner, and moves to the runtime
	// cap.
	let other = queue(store, 8, false).await;
	store.step_ci_runner(&other, None, &idle).await.unwrap();
	store
		.step_ci_runner(&other, None, &RunnerStep::Stop(StopReason::Expired))
		.await
		.unwrap();
	let runner = store.ci_runner(&other).await.unwrap().unwrap();
	assert_eq!(runner.stop, Some(StopReason::Expired));
	let third = queue(store, 9, false).await;
	store.step_ci_runner(&third, None, &idle).await.unwrap();
	store
		.step_ci_runner(&third, None, &RunnerStep::Revive)
		.await
		.unwrap();
	assert_eq!(store.ci_runner(&third).await.unwrap().unwrap().state, RunnerState::Idle);
	db.finish().await;
}

#[test]
fn public_policy_needs_both_actors() {
	let policy = PublicPolicy { enabled: true, anyone: false, users: vec!["Alice".into()] };
	assert!(policy.allows("alice", "ALICE"));
	assert!(!policy.allows("alice", "mallory"));
	assert!(PublicPolicy { anyone: true, ..policy.clone() }.allows("x", "y"));
	assert!(!PublicPolicy { enabled: false, ..policy }.allows("alice", "alice"));
}
