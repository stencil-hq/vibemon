//! Template spawn statistics (`spawnCount`, `lastSpawnedAt`), written off the
//! sandbox create path.
//!
//! A create only counts its spawn in memory; one background task writes the
//! accumulated counts, one store transaction per template at a time. Creates
//! therefore never wait on the template record, which concurrent creates of
//! one template would otherwise contend on (each write is a raft commit, and
//! concurrent ones rerun on conflict). Upstream batches the same way
//! (`TemplateSpawnCounter`).

use std::{collections::HashMap, time::Duration};

use chrono::{DateTime, Utc};
use parking_lot::Mutex;
use tokio::sync::Notify;
use vmon_cluster::kv::KvError;

use crate::{state::AppState, store};

/// Delay before the writer retries counts whose write failed, doubling up to
/// [`MAX_RETRY_DELAY`] while failures continue.
const RETRY_DELAY: Duration = Duration::from_secs(1);
const MAX_RETRY_DELAY: Duration = Duration::from_secs(30);

/// Spawns of one template not yet written.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Spawns {
	count: u64,
	last:  DateTime<Utc>,
}

/// Spawn counts awaiting the writer ([`spawn_spawn_writer`]).
#[derive(Debug, Default)]
pub struct SpawnCounter {
	pending: Mutex<HashMap<String, Spawns>>,
	wake:    Notify,
}

impl SpawnCounter {
	/// Counts one sandbox start from `template_id` now.
	pub fn record(&self, template_id: &str) {
		self.add(template_id, Spawns { count: 1, last: Utc::now() });
		self.wake.notify_one();
	}

	fn add(&self, template_id: &str, spawns: Spawns) {
		self
			.pending
			.lock()
			.entry(template_id.to_owned())
			.and_modify(|pending| {
				pending.count += spawns.count;
				pending.last = pending.last.max(spawns.last);
			})
			.or_insert(spawns);
	}

	fn take(&self) -> HashMap<String, Spawns> {
		std::mem::take(&mut *self.pending.lock())
	}
}

/// Starts the task writing recorded spawns to the store (called by
/// [`crate::Driver::start`]). Counts whose write failed are retried with
/// backoff, without waiting for the next create.
pub fn spawn_spawn_writer(state: AppState) {
	tokio::spawn(async move {
		let mut retry = RETRY_DELAY;
		loop {
			state.spawns.wake.notified().await;
			while !write_pending(&state).await {
				tokio::time::sleep(retry).await;
				retry = (retry * 2).min(MAX_RETRY_DELAY);
			}
			retry = RETRY_DELAY;
		}
	});
}

/// Writes the counts still pending (called on driver shutdown so recorded
/// spawns are not lost); `false` when some write failed.
pub async fn flush_spawns(state: &AppState) -> bool {
	write_pending(state).await
}

/// Writes every pending count; `false` when a template's counts were kept for
/// another attempt.
async fn write_pending(state: &AppState) -> bool {
	let mut written = true;
	for (template_id, spawns) in state.spawns.take() {
		let Err(error) = state
			.store
			.add_template_spawns(&template_id, spawns.count, spawns.last)
			.await
		else {
			continue;
		};
		if unapplied(&error) {
			tracing::warn!(template = %template_id, %error, "spawn count update failed; retrying");
			state.spawns.add(&template_id, spawns);
			written = false;
		} else {
			// The commit may have applied before the call failed; replaying
			// it could count these spawns twice.
			tracing::warn!(
				template = %template_id,
				count = spawns.count,
				%error,
				"spawn count update outcome unknown; dropping it"
			);
		}
	}
	written
}

/// Whether `error` proves [`store::Store::add_template_spawns`] wrote nothing:
/// every commit attempt lost its guards (`Contention`), a read failed before
/// any commit (`Corrupt`, `Duplicate`), or the KV refused the call before
/// proposing it (`NoLeader`: no leader accepted the proposal; `Rejected`).
/// Other KV failures may follow a proposal that still commits.
const fn unapplied(error: &store::Error) -> bool {
	matches!(
		error,
		store::Error::Contention(_)
			| store::Error::Corrupt { .. }
			| store::Error::Duplicate(_)
			| store::Error::Kv(KvError::NoLeader | KvError::Rejected(_))
	)
}

#[cfg(test)]
mod tests {
	use chrono::TimeDelta;

	use super::*;

	#[test]
	fn spawns_coalesce_per_template_until_taken() {
		let counter = SpawnCounter::default();
		for _ in 0..8 {
			counter.record("a");
		}
		counter.record("b");
		let taken = counter.take();
		assert_eq!(taken.len(), 2);
		assert_eq!(taken["a"].count, 8);
		assert_eq!(taken["b"].count, 1);
		assert!(counter.take().is_empty());
	}

	#[test]
	fn requeued_counts_merge_and_keep_the_latest_spawn() {
		let counter = SpawnCounter::default();
		let earlier = Utc::now() - TimeDelta::minutes(1);
		counter.record("a");
		let recorded = counter.take()["a"].last;
		counter.add("a", Spawns { count: 3, last: earlier });
		counter.add("a", Spawns { count: 1, last: recorded });
		assert_eq!(counter.take()["a"], Spawns { count: 4, last: recorded });
	}
}
