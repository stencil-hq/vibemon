//! In-memory fleet history for the admin dashboard (`GET /admin/cluster`).
//!
//! Every driver samples gossip capacity and the `sandbox_events` start count
//! every [`SAMPLE_INTERVAL`] and keeps the newest [`HISTORY_LEN`] samples.

use std::{collections::VecDeque, time::Duration};

use parking_lot::Mutex;
use serde::Serialize;
use vmon_cluster::{
	MemberMeta,
	gossip::{Member, MemberState},
};

use crate::{lifecycle::now_ms, state::AppState};

/// Time between samples.
pub const SAMPLE_INTERVAL: Duration = Duration::from_secs(5);
/// Samples kept (1 h at [`SAMPLE_INTERVAL`]).
pub const HISTORY_LEN: usize = 720;

/// One fleet-wide sample; capacity sums cover alive node members only.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FleetSample {
	/// Unix ms.
	pub ts:             u64,
	pub nodes:          u32,
	pub sandboxes:      u64,
	pub builds:         u64,
	pub vcpus:          u64,
	pub vcpus_used:     u64,
	#[serde(rename = "memoryMB")]
	pub memory_mb:      u64,
	#[serde(rename = "memoryUsedMB")]
	pub memory_used_mb: u64,
	/// Sandbox creates + resumes recorded since the previous sample.
	pub starts:         u64,
}

/// Bounded ring of [`FleetSample`]s, oldest first.
#[derive(Debug, Default)]
pub struct FleetStats {
	samples: Mutex<VecDeque<FleetSample>>,
}

impl FleetStats {
	/// Every kept sample, oldest first.
	pub fn snapshot(&self) -> Vec<FleetSample> {
		self.samples.lock().iter().copied().collect()
	}

	/// Appends a sample, evicting the oldest beyond [`HISTORY_LEN`].
	pub fn push(&self, sample: FleetSample) {
		let mut samples = self.samples.lock();
		if samples.len() == HISTORY_LEN {
			samples.pop_front();
		}
		samples.push_back(sample);
	}
}

/// Sums the capacity of alive node members into a sample.
pub fn aggregate<'a>(
	members: impl IntoIterator<Item = (&'a Member, &'a MemberMeta)>,
	ts: u64,
	starts: u64,
) -> FleetSample {
	let mut sample = FleetSample { ts, starts, ..FleetSample::default() };
	for (member, meta) in members {
		let Some(cap) = meta.node.as_ref().filter(|_| meta.roles.node) else {
			continue;
		};
		if member.state != MemberState::Alive {
			continue;
		}
		sample.nodes += 1;
		sample.sandboxes += u64::from(cap.sandboxes);
		sample.builds += u64::from(cap.builds);
		sample.vcpus += u64::from(cap.cpus);
		sample.vcpus_used += u64::from(cap.vcpus_used);
		sample.memory_mb += cap.memory_mb;
		sample.memory_used_mb += cap.memory_used_mb;
	}
	sample
}

/// Starts the sampler loop feeding
/// [`Inner::stats`](crate::state::Inner::stats).
pub fn spawn_sampler(state: AppState) {
	tokio::spawn(async move {
		let mut cursor: Option<u64> = None;
		let mut tick = tokio::time::interval(SAMPLE_INTERVAL);
		tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
		loop {
			tick.tick().await;
			let starts = count_starts(&state, &mut cursor).await;
			let members = state.cluster.members();
			let sample = aggregate(members.iter().map(|(m, meta)| (m, meta)), now_ms(), starts);
			state.stats.push(sample);
		}
	});
}

/// Start events since `cursor`, advancing it; 0 (cursor kept) on DB errors.
/// The first successful call only initializes the cursor.
async fn count_starts(state: &AppState, cursor: &mut Option<u64>) -> u64 {
	let Some(after) = *cursor else {
		match state.store.max_event_seq().await {
			Ok(seq) => *cursor = Some(seq),
			Err(error) => tracing::debug!(%error, "fleet sampler: reading event cursor failed"),
		}
		return 0;
	};
	match state.store.start_events_after(after).await {
		Ok((starts, max_seq)) => {
			*cursor = Some(max_seq);
			starts
		},
		Err(error) => {
			tracing::debug!(%error, "fleet sampler: counting start events failed");
			0
		},
	}
}

#[cfg(test)]
mod tests {
	use std::net::SocketAddr;

	use vmon_cluster::{NodeCapacity, NodeId, Roles};

	use super::*;

	fn member(id: u64, state: MemberState) -> Member {
		Member {
			id: NodeId(id),
			addr: SocketAddr::from(([127, 0, 0, 1], 7946)),
			state,
			incarnation: 0,
			meta_version: 0,
			meta: Vec::new(),
		}
	}

	fn node_meta(sandboxes: u32, cpus: u32, memory_mb: u64) -> MemberMeta {
		MemberMeta {
			roles: Roles { node: true, driver: false },
			node: Some(NodeCapacity {
				cpus,
				memory_mb,
				vcpus_used: 2,
				memory_used_mb: 512,
				sandboxes,
				builds: 1,
				..NodeCapacity::default()
			}),
			..MemberMeta::default()
		}
	}

	#[test]
	fn aggregate_sums_alive_node_members_only() {
		let driver =
			MemberMeta { roles: Roles { node: false, driver: true }, ..MemberMeta::default() };
		let members = [
			(member(1, MemberState::Alive), node_meta(3, 8, 16_384)),
			(member(2, MemberState::Alive), node_meta(1, 4, 8192)),
			(member(3, MemberState::Dead), node_meta(9, 64, 1 << 20)),
			(member(4, MemberState::Suspect), node_meta(9, 64, 1 << 20)),
			(member(5, MemberState::Alive), driver),
		];
		let sample = aggregate(members.iter().map(|(m, meta)| (m, meta)), 42, 7);
		assert_eq!(sample, FleetSample {
			ts:             42,
			nodes:          2,
			sandboxes:      4,
			builds:         2,
			vcpus:          12,
			vcpus_used:     4,
			memory_mb:      24_576,
			memory_used_mb: 1024,
			starts:         7,
		});
	}

	#[test]
	fn ring_keeps_newest_samples_oldest_first() {
		let stats = FleetStats::default();
		for ts in 0..(HISTORY_LEN as u64 + 5) {
			stats.push(FleetSample { ts, ..FleetSample::default() });
		}
		let kept = stats.snapshot();
		assert_eq!(kept.len(), HISTORY_LEN);
		assert_eq!(kept.first().map(|s| s.ts), Some(5));
		assert_eq!(kept.last().map(|s| s.ts), Some(HISTORY_LEN as u64 + 4));
	}
}
