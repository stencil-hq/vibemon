//! Build supervisor (ARCHITECTURE.md §9.4).
//!
//! Every driver scans `MySQL` for builds in `building`; for each it can
//! lease (`lock/build/<id>`, TTL,
//! renewed) it polls the builder node's `GET /node/v1/builds/{id}`,
//! persists new log lines, and records the outcome: on `ready` the
//! artifact (`MySQL` + KV `art/<build>/<node>`) and tags, on failure the
//! reason. A build whose lease lapses (its driver died) is adopted by any
//! driver; a builder that gossip declares dead fails the build; builds
//! exceeding [`BUILD_TIMEOUT`] are cancelled with the upstream message.
//! The raft leader additionally runs artifact GC, fails snapshot builds
//! abandoned mid-way, retires fork templates their fork never retired, and
//! purges fork templates whose artifacts are collected.

use std::{
	collections::HashSet,
	sync::Arc,
	time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
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
	state::AppState,
	store::builds::{BuildRow, BuildStatus, LogLevel, NewLog},
};

/// Timing knobs of the supervisor (tests shorten them).
#[derive(Clone, Copy, Debug)]
pub(super) struct Timing {
	/// How often `MySQL` is scanned for unsupervised builds.
	pub scan:           Duration,
	/// How often a supervised build's node is polled.
	pub poll:           Duration,
	/// Lease TTL of `lock/build/<id>`.
	pub lock_ttl:       Duration,
	/// How often the lease is renewed.
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
					match try_lock(&state, &id, timing.lock_ttl).await {
						Ok(true) => {
							supervised.lock().insert(id.clone());
							let state = state.clone();
							let supervised = Arc::clone(&supervised);
							tokio::spawn(async move {
								supervise(&state, &id, timing).await;
								supervised.lock().remove(&id);
							});
						},
						Ok(false) => {},
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

/// Takes (or re-takes, when this driver already holds it) a build lease.
async fn try_lock(
	state: &AppState,
	build_id: &str,
	ttl: Duration,
) -> Result<bool, vmon_cluster::kv::KvError> {
	let key = lock_key(build_id);
	let me = state.cluster.local_id().to_string().into_bytes();
	let ttl_ms = Some(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
	let put = Op::Put { key: key.clone(), value: me.clone(), ttl_ms };
	let fresh = Txn {
		compare: vec![Compare::Missing(key.clone())],
		success: vec![put.clone()],
		failure: vec![],
	};
	if state.cluster.kv.txn(fresh).await?.succeeded {
		return Ok(true);
	}
	renew(state, build_id, ttl).await
}

/// Extends a lease held by this driver; `false` when another holder has it.
async fn renew(
	state: &AppState,
	build_id: &str,
	ttl: Duration,
) -> Result<bool, vmon_cluster::kv::KvError> {
	let key = lock_key(build_id);
	let me = state.cluster.local_id().to_string().into_bytes();
	let ttl_ms = Some(u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX));
	let txn = Txn {
		compare: vec![Compare::Value { key: key.clone(), value: me.clone() }],
		success: vec![Op::Put { key, value: me, ttl_ms }],
		failure: vec![],
	};
	Ok(state.cluster.kv.txn(txn).await?.succeeded)
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

async fn supervise(state: &AppState, build_id: &str, timing: Timing) {
	tracing::debug!(build = build_id, "supervising build");
	let mut logs_offset = match state.store.build_log_count(build_id).await {
		Ok(count) => count,
		Err(error) => {
			tracing::warn!(build = build_id, %error, "reading build log count failed");
			release(state, build_id).await;
			return;
		},
	};
	let mut last_renewed = Instant::now();
	let mut poll = tokio::time::interval(timing.poll);
	poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
	loop {
		poll.tick().await;
		if last_renewed.elapsed() >= timing.renew {
			match renew(state, build_id, timing.lock_ttl).await {
				Ok(true) => last_renewed = Instant::now(),
				Ok(false) => {
					tracing::info!(build = build_id, "build lease taken over by another driver");
					return;
				},
				Err(error) if last_renewed.elapsed() + timing.renew >= timing.lock_ttl => {
					tracing::warn!(build = build_id, %error, "build lease about to lapse; stopping supervision");
					return;
				},
				Err(error) => {
					tracing::debug!(build = build_id, %error, "build lease renewal failed; retrying");
				},
			}
		}
		match poll_once(state, build_id, &mut logs_offset).await {
			Ok(Progress::Running) => {},
			Ok(Progress::Done) => break,
			Err(error) => tracing::warn!(build = build_id, %error, "build poll failed"),
		}
	}
	release(state, build_id).await;
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
		Some((member, meta)) if member.state == MemberState::Alive => meta.cluster_url,
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
			BuildState::Waiting | BuildState::Building => Ok(Progress::Running),
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

/// Whether a build has run longer than [`BUILD_TIMEOUT`].
fn timed_out(build: &BuildRow, now: DateTime<Utc>) -> bool {
	let started = build.started_at.unwrap_or(build.created_at);
	now.signed_duration_since(started)
		.to_std()
		.is_ok_and(|elapsed| elapsed > BUILD_TIMEOUT)
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
			tags:             sqlx::types::Json(vec![]),
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
		assert!(timed_out(&build(now - hour - second, None), now), "falls back to creation time");
	}
}
