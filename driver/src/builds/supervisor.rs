//! Build supervisor (ARCHITECTURE.md §9.4).
//!
//! Every driver scans the store for builds in `building`; for each it can
//! lease (`lock/build/<id>`, TTL, renewed independently of how long polls
//! take; a lapsed lease nobody else took is re-taken) it polls the builder
//! node's `GET /node/v1/builds/{id}`,
//! persists new log lines, and records the outcome: on `ready` the
//! artifact (store + KV `art/<build>/<node>`) and tags, on failure the
//! reason. A build whose lease lapses (its driver died) is adopted by any
//! driver, and losing the lease to another driver stops supervision; a
//! builder that gossip declares dead fails the build; builds running longer
//! than [`BUILD_TIMEOUT`] on their builder (time queued there is free) are
//! cancelled with the upstream message.
//! The raft leader additionally runs artifact GC, fails snapshot builds
//! abandoned mid-way, retires fork templates their fork never retired, and
//! purges fork templates whose artifacts are collected.

use std::{collections::HashSet, sync::Arc, time::Duration};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use tokio::time::Instant;
use vmon_cluster::{
	NodeId,
	gossip::MemberState,
	kv::{Compare, Op, Txn},
};
use vmon_node::api::{BuildReason, BuildState};

use super::{
	BUILD_TIMEOUT, BUILD_TIMEOUT_MESSAGE, artifacts, lock_key, record_artifact, snapshots,
};
use crate::{
	lifecycle::routes::{Lease, hold_lease},
	state::AppState,
	store::builds::{BuildRow, BuildStatus, LogLevel, NewLog},
};

/// Timing knobs of the supervisor (tests shorten them).
#[derive(Clone, Copy, Debug)]
pub(super) struct Timing {
	/// How often the store is scanned for unsupervised builds.
	pub scan:           Duration,
	/// How often a supervised build's node is polled.
	pub poll:           Duration,
	/// Lease TTL of `lock/build/<id>`.
	pub lock_ttl:       Duration,
	/// How often the lease is renewed (counted from the previous successful
	/// renewal's proposal, whatever the polls are doing).
	pub renew:          Duration,
	/// How often the leader runs artifact GC and stale-snapshot cleanup.
	pub housekeeping:   Duration,
	/// Age after which a `building` snapshot build, or a fork template not
	/// yet retired, counts as abandoned.
	pub snapshot_stale: Duration,
}

impl Default for Timing {
	fn default() -> Self {
		Self {
			scan:           Duration::from_secs(1),
			poll:           Duration::from_millis(500),
			lock_ttl:       Duration::from_secs(20),
			renew:          Duration::from_secs(5),
			housekeeping:   Duration::from_mins(1),
			snapshot_stale: Duration::from_mins(30),
		}
	}
}

/// Starts the supervisor loop for this driver (called once by
/// [`crate::Driver::start`]).
pub fn spawn_supervisor(state: AppState) {
	tokio::spawn(run(state, Timing::default()));
}

pub(super) async fn run(state: AppState, timing: Timing) {
	let supervised: Arc<Mutex<HashSet<String>>> = Arc::default();
	let mut scan = tokio::time::interval(timing.scan);
	scan.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	let mut last_housekeeping: Option<Instant> = None;
	loop {
		scan.tick().await;
		match state.store.building_build_ids().await {
			Ok(ids) => {
				for id in ids {
					if supervised.lock().contains(&id) {
						continue;
					}
					let proposed = Instant::now();
					match lease(&state, &id, timing.lock_ttl).await {
						Ok(Lease::Extended | Lease::Taken) => {
							supervised.lock().insert(id.clone());
							let state = state.clone();
							let supervised = Arc::clone(&supervised);
							tokio::spawn(async move {
								supervise(&state, &id, timing, proposed).await;
								supervised.lock().remove(&id);
							});
						},
						Ok(Lease::Lost) => {},
						Err(error) => tracing::debug!(build = %id, %error, "build lease unavailable"),
					}
				}
			},
			Err(error) => tracing::warn!(%error, "scanning builds failed"),
		}
		if state.cluster.is_leader()
			&& last_housekeeping.is_none_or(|at| at.elapsed() >= timing.housekeeping)
		{
			last_housekeeping = Some(Instant::now());
			housekeeping(&state, timing.snapshot_stale).await;
		}
	}
}

/// One leader housekeeping pass: fails snapshot builds and retires fork
/// templates abandoned for longer than `stale_after`, collects the artifacts
/// of deleted templates no sandbox uses, then purges the rows of fork
/// templates whose artifacts are gone.
pub async fn housekeeping(state: &AppState, stale_after: Duration) {
	fail_stale_snapshots(state, stale_after).await;
	let before = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
	if let Err(error) = snapshots::retire_abandoned_forks(state, before).await {
		tracing::warn!(%error, "retiring abandoned fork templates failed");
	}
	if let Err(error) = artifacts::collect_garbage(state).await {
		tracing::warn!(%error, "artifact gc pass failed");
	}
	match state.store.purge_collected_forks().await {
		Ok(0) => {},
		Ok(purged) => tracing::info!(templates = purged, "purged collected fork templates"),
		Err(error) => tracing::warn!(%error, "purging collected fork templates failed"),
	}
}

/// Takes, extends, or re-takes (after a lapse) this driver's build lease.
async fn lease(
	state: &AppState,
	build_id: &str,
	ttl: Duration,
) -> Result<Lease, vmon_cluster::kv::KvError> {
	let me = state.cluster.local_id().to_string().into_bytes();
	hold_lease(&state.cluster.kv, &lock_key(build_id), &me, ttl).await
}

/// Keeps this driver's build lease alive for as long as supervision runs:
/// renews every `timing.renew` after the last successful write was proposed
/// (`held` initially), however long the polls take. Returns when the lease
/// is gone: another driver took it, or the KV stayed unreachable until it
/// may have lapsed.
async fn keep_lease(state: &AppState, build_id: &str, timing: Timing, mut held: Instant) {
	let mut next = held + timing.renew;
	loop {
		tokio::time::sleep_until(next).await;
		let proposed = Instant::now();
		match lease(state, build_id, timing.lock_ttl).await {
			Ok(Lease::Extended) => held = proposed,
			Ok(Lease::Taken) => {
				tracing::info!(
					build = build_id,
					"build lease lapsed before its renewal applied; re-took it"
				);
				held = proposed;
			},
			Ok(Lease::Lost) => {
				tracing::info!(build = build_id, "build lease taken over by another driver");
				return;
			},
			Err(error) if held.elapsed() >= timing.lock_ttl => {
				tracing::warn!(build = build_id, %error, "build lease lapsed; stopping supervision");
				return;
			},
			Err(error) => {
				tracing::debug!(build = build_id, %error, "build lease renewal failed; retrying");
				next = Instant::now() + timing.poll;
				continue;
			},
		}
		next = held + timing.renew;
	}
}

async fn release(state: &AppState, build_id: &str) {
	let key = lock_key(build_id);
	let me = state.cluster.local_id().to_string().into_bytes();
	let txn = Txn {
		compare: vec![Compare::Value { key: key.clone(), value: me }],
		success: vec![Op::Delete { key }],
		failure: vec![],
	};
	if let Err(error) = state.cluster.kv.txn(txn).await {
		tracing::debug!(build = build_id, %error, "releasing build lease failed (it will expire)");
	}
}

/// Outcome of one poll.
#[derive(Debug, PartialEq, Eq)]
enum Progress {
	/// Still running (or temporarily unobservable); poll again.
	Running,
	/// The build reached a terminal state (or is no longer ours to watch).
	Done,
}

/// Supervises a build whose lease this driver took with a write proposed at
/// `held`. Polling and lease renewal run side by side, so a poll stalled on
/// the store or the node cannot starve the renewal; losing the lease abandons
/// the poll in flight (every step of it is idempotent for the new holder).
async fn supervise(state: &AppState, build_id: &str, timing: Timing, held: Instant) {
	tracing::debug!(build = build_id, "supervising build");
	let polling = async {
		let mut logs_offset = match state.store.build_log_count(build_id).await {
			Ok(count) => count,
			Err(error) => {
				tracing::warn!(build = build_id, %error, "reading build log count failed");
				return;
			},
		};
		let mut poll = tokio::time::interval(timing.poll);
		poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		loop {
			poll.tick().await;
			match poll_once(state, build_id, &mut logs_offset).await {
				Ok(Progress::Running) => {},
				Ok(Progress::Done) => return,
				Err(error) => tracing::warn!(build = build_id, %error, "build poll failed"),
			}
		}
	};
	tokio::select! {
		() = polling => release(state, build_id).await,
		() = keep_lease(state, build_id, timing, held) => {},
	}
}

/// One supervision step for a build.
async fn poll_once(
	state: &AppState,
	build_id: &str,
	logs_offset: &mut u64,
) -> anyhow::Result<Progress> {
	let Some(build) = state.store.build(build_id).await? else {
		return Ok(Progress::Done);
	};
	if build.status() != BuildStatus::Building {
		return Ok(Progress::Done);
	}
	let node_id = build
		.node_id
		.as_deref()
		.and_then(|id| id.parse::<NodeId>().ok());
	if timed_out(&build, Utc::now()) {
		if let Some(node_id) = node_id
			&& let Ok(client) = state.node(node_id)
		{
			let _ = client.cancel_build(build_id).await;
		}
		fail(state, build_id, BUILD_TIMEOUT_MESSAGE, None).await?;
		return Ok(Progress::Done);
	}
	let Some(node_id) = node_id else {
		fail(state, build_id, "build has no builder node", None).await?;
		return Ok(Progress::Done);
	};
	let member = state
		.cluster
		.members()
		.into_iter()
		.find(|(member, meta)| member.id == node_id && meta.roles.node);
	let url = match member {
		Some((member, meta)) if member.state == MemberState::Alive => meta.cluster_url(&member),
		Some((member, _)) if member.state == MemberState::Suspect => return Ok(Progress::Running),
		_ => {
			let message = format!("builder node {node_id} is no longer available");
			fail(state, build_id, &message, None).await?;
			return Ok(Progress::Done);
		},
	};
	let client = state
		.node_at(&url)
		.map_err(|e| anyhow::anyhow!(e.message))?;
	loop {
		let status = match client.build_status(build_id, *logs_offset).await {
			Ok(status) => status,
			Err(error) if error.is_not_found() => {
				fail(state, build_id, &format!("builder node {node_id} lost the build"), None).await?;
				return Ok(Progress::Done);
			},
			// Transport trouble: gossip decides whether the node is gone.
			Err(error) => {
				tracing::debug!(build = build_id, %error, "builder not reachable");
				return Ok(Progress::Running);
			},
		};
		let fetched = status.logs.len() as u64;
		if fetched > 0 {
			let logs: Vec<NewLog> = status
				.logs
				.iter()
				.map(|entry| NewLog {
					ts:      DateTime::from_timestamp_millis(
						i64::try_from(entry.timestamp_ms).unwrap_or(i64::MAX),
					)
					.unwrap_or_else(Utc::now),
					level:   LogLevel::normalize(&entry.level),
					step:    entry.step.clone().filter(|s| !s.is_empty()),
					message: entry.message.clone(),
				})
				.collect();
			state
				.store
				.append_build_logs(build_id, *logs_offset, &logs)
				.await?;
			*logs_offset += fetched;
		}
		// Drain every log line before recording a terminal state, so a
		// client that sees `ready`/`error` can page to the end.
		if fetched > 0 && *logs_offset < status.logs_total {
			continue;
		}
		return match status.status {
			BuildState::Waiting => Ok(Progress::Running),
			BuildState::Building => {
				// The build left the builder's queue: its time limit starts now.
				if build.started_at.is_none() {
					state.store.mark_build_running(build_id).await?;
				}
				Ok(Progress::Running)
			},
			BuildState::Ready => {
				let Some(artifact) = status.artifact else {
					fail(state, build_id, "builder reported ready without an artifact", None).await?;
					return Ok(Progress::Done);
				};
				record_artifact(state, &artifact)
					.await
					.map_err(|e| anyhow::anyhow!(e.message))?;
				if state.store.finish_build_ready(build_id, &artifact).await? {
					tracing::info!(build = build_id, node = %node_id, "build ready");
				}
				Ok(Progress::Done)
			},
			BuildState::Error | BuildState::Cancelled => {
				let reason = status.reason.unwrap_or_else(|| BuildReason {
					message: if status.status == BuildState::Cancelled {
						"The build was cancelled.".into()
					} else {
						"The build failed.".into()
					},
					step:    None,
				});
				state.store.fail_build(build_id, &reason).await?;
				Ok(Progress::Done)
			},
		};
	}
}

/// Whether a build has run longer than [`BUILD_TIMEOUT`] on its builder.
/// Time spent waiting, in the store or in the builder's queue (`started_at`
/// still unset), does not count.
fn timed_out(build: &BuildRow, now: DateTime<Utc>) -> bool {
	build.started_at.is_some_and(|started| {
		now.signed_duration_since(started)
			.to_std()
			.is_ok_and(|elapsed| elapsed > BUILD_TIMEOUT)
	})
}

async fn fail(
	state: &AppState,
	build_id: &str,
	message: &str,
	step: Option<String>,
) -> anyhow::Result<()> {
	tracing::warn!(build = build_id, message, "build failed");
	state
		.store
		.fail_build(build_id, &BuildReason { message: message.to_owned(), step })
		.await?;
	Ok(())
}

async fn fail_stale_snapshots(state: &AppState, stale_after: Duration) {
	let before = Utc::now() - chrono::Duration::from_std(stale_after).unwrap_or_default();
	match state.store.stale_snapshot_builds(before).await {
		Ok(ids) => {
			for id in ids {
				let reason =
					BuildReason { message: "The snapshot was interrupted.".into(), step: None };
				if let Err(error) = state.store.fail_build(&id, &reason).await {
					tracing::warn!(build = %id, %error, "failing stale snapshot build");
				}
			}
		},
		Err(error) => tracing::warn!(%error, "scanning stale snapshot builds failed"),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn build(created: DateTime<Utc>, started: Option<DateTime<Utc>>) -> BuildRow {
		BuildRow {
			id:               "b".into(),
			template_id:      "t".into(),
			team_id:          "team".into(),
			status:           "building".into(),
			kind:             "build".into(),
			vcpus:            2,
			memory_mb:        1024,
			disk_mb:          1024,
			min_free_disk_mb: None,
			envd_version:     None,
			arch:             None,
			node_id:          None,
			tags:             vec![],
			spec:             None,
			template_version: None,
			start_cmd:        None,
			ready_cmd:        None,
			reason:           None,
			artifact:         None,
			created_at:       created,
			updated_at:       created,
			started_at:       started,
			finished_at:      None,
			artifacts_gc_at:  None,
		}
	}

	#[test]
	fn timeout_counts_from_start_and_is_exclusive_at_one_hour() {
		let now = Utc::now();
		let hour = chrono::Duration::hours(1);
		let second = chrono::Duration::seconds(1);
		assert!(
			!timed_out(&build(now - hour * 2, Some(now - hour)), now),
			"exactly 1h is still allowed"
		);
		assert!(timed_out(&build(now - hour * 2, Some(now - hour - second)), now));
		assert!(
			!timed_out(&build(now - hour * 2, Some(now - second)), now),
			"waiting time before start is free"
		);
		assert!(
			!timed_out(&build(now - hour * 3, None), now),
			"queued on the builder: not started yet"
		);
	}
}
