//! Sandbox placement (ARCHITECTURE.md §9.3): filter alive node members by
//! admin status, features, architecture, and capacity, score the survivors,
//! and pick one with power-of-two-choices.
//!
//! The filtering and scoring functions are pure so they can be unit-tested;
//! [`Scheduler`] keeps the driver-local counters (in-flight creates and
//! create outcomes per node) that feed the score and `GET /nodes`.

use std::{
	collections::{HashMap, HashSet},
	hash::BuildHasher,
};

use parking_lot::Mutex;
use rand::RngExt;
use serde::{Deserialize, Serialize};
use vmon_cluster::{
	MemberMeta, NodeId,
	kv::{Kv, KvError},
};
use vmon_node::paused_store::{self, Remote};

/// vCPU overcommit factor: a node accepts sandboxes until the allocated vCPUs
/// reach this multiple of its physical CPUs.
pub const VCPU_OVERCOMMIT: u64 = 4;
/// Memory overcommit factor: template forks share memory copy-on-write, so the
/// allocated guest memory may exceed physical memory by this factor.
pub const MEMORY_OVERCOMMIT: u64 = 2;
/// Nodes with less free disk than this are not eligible.
pub const MIN_FREE_DISK_MB: u64 = 512;
/// Guest memory used to express node capacity as a sandbox count (`GET
/// /nodes`).
pub const NOMINAL_SANDBOX_MEMORY_MB: u64 = 512;
/// Placement attempts per create before giving up (§9.3).
pub const MAX_ATTEMPTS: usize = 3;

/// Score added when the node already holds the build's artifacts.
const ARTIFACT_BONUS: f64 = 1.0;
/// Extra score when the local artifact carries a reusable memory snapshot
/// (same backend and CPU baseline): the create is a `CoW` fork, not a reboot.
const SNAPSHOT_BONUS: f64 = 0.5;
/// Score removed per create already in flight on the node.
const INFLIGHT_PENALTY: f64 = 0.25;

/// Admin status of a node (KV `node/<id>/status`, E2B `NodeStatus` subset we
/// accept).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum NodeStatus {
	#[default]
	Ready,
	Draining,
	Unhealthy,
}

impl NodeStatus {
	/// Wire name used by the E2B admin API and the KV value.
	pub const fn as_str(self) -> &'static str {
		match self {
			Self::Ready => "ready",
			Self::Draining => "draining",
			Self::Unhealthy => "unhealthy",
		}
	}
}

/// Prefix of the admin node status keys.
pub const STATUS_PREFIX: &str = "node/";

/// KV key `node/<nodeID>/status` (written by `POST /nodes/{nodeID}`).
pub fn status_key(node: NodeId) -> String {
	format!("{STATUS_PREFIX}{node}/status")
}

/// Admin status overrides of every node (absent = ready).
pub async fn load_statuses(kv: &Kv) -> Result<HashMap<NodeId, NodeStatus>, KvError> {
	Ok(kv
		.list(STATUS_PREFIX)
		.await?
		.into_iter()
		.filter_map(|entry| {
			let id = entry
				.key
				.strip_prefix(STATUS_PREFIX)?
				.strip_suffix("/status")?
				.parse()
				.ok()?;
			Some((id, serde_json::from_slice(&entry.value).ok()?))
		})
		.collect())
}

/// Where a build's artifacts already live (subset of `ArtifactInfo`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ArtifactLocation {
	pub node_id:      NodeId,
	pub backend:      String,
	pub cpu_baseline: String,
	pub has_snapshot: bool,
}

/// What a sandbox needs from a node.
#[derive(Clone, Debug, Default)]
pub struct PlacementRequest {
	pub vcpus:     u32,
	pub memory_mb: u64,
	/// Guest architecture of the build (`x86_64`/`aarch64`); empty = any.
	pub arch:      String,
	/// Node features that must all be present (`egress-proxy`, …).
	pub features:  Vec<&'static str>,
	/// Hard pin (volume locality, fork, paused-state resume).
	pub pinned:    Option<NodeId>,
	/// Nodes holding the build's artifacts.
	pub artifacts: Vec<ArtifactLocation>,
	/// Nodes already tried for this create.
	pub excluded:  Vec<NodeId>,
}

/// An eligible node with its score.
#[derive(Clone, Debug)]
pub struct Candidate {
	pub node_id:     NodeId,
	pub cluster_url: String,
	pub score:       f64,
}

/// Why no node was eligible; maps to the E2B placement `error_code`s.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Unplaceable {
	/// No node matches arch/features/pin (`sandbox_no_compatible_node`).
	NoCompatibleNode,
	/// Compatible nodes exist but none has room
	/// (`sandbox_capacity_unavailable`).
	NoCapacity,
}

/// Driver-local placement counters, shared through `AppState::scheduler`.
#[derive(Debug, Default)]
pub struct Scheduler {
	counters: Mutex<HashMap<NodeId, NodeCounters>>,
}

/// Create counters for one node.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct NodeCounters {
	pub inflight:  u32,
	pub successes: u64,
	pub fails:     u64,
}

/// Marks one create as in flight on a node until [`InflightGuard::finish`] or
/// drop.
pub struct InflightGuard<'a> {
	scheduler: &'a Scheduler,
	node:      NodeId,
	done:      bool,
}

/// Outcome recorded by [`InflightGuard::finish`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CreateOutcome {
	Success,
	Failure,
}

impl Scheduler {
	/// Snapshot of every node's counters (GET /nodes and scoring).
	pub fn counters(&self) -> HashMap<NodeId, NodeCounters> {
		self.counters.lock().clone()
	}

	/// Counters for one node.
	pub fn node_counters(&self, node: NodeId) -> NodeCounters {
		self.counters.lock().get(&node).copied().unwrap_or_default()
	}

	/// Registers a create in flight on `node`; called by the lifecycle create
	/// path.
	pub fn begin(&self, node: NodeId) -> InflightGuard<'_> {
		self.counters.lock().entry(node).or_default().inflight += 1;
		InflightGuard { scheduler: self, node, done: false }
	}

	/// Prefers the paused owner, then a snapshot-compatible node. Only falls
	/// back to a rootfs-only boot when no eligible snapshot-compatible node
	/// exists. `members` must contain alive nodes only; volume pins remain hard
	/// filters.
	pub fn place_paused<S: BuildHasher>(
		&self,
		members: &[(NodeId, MemberMeta)],
		statuses: &HashMap<NodeId, NodeStatus, S>,
		req: &PlacementRequest,
		owner: NodeId,
		remote: &Remote,
		cold_boot: bool,
	) -> Result<(Candidate, bool), Unplaceable> {
		let counters = self.counters();
		let mut candidates =
			eligible(members, statuses, req, |id| counters.get(&id).map_or(0, |c| c.inflight))?;
		candidates.retain(|candidate| {
			candidate.node_id == owner
				|| members.iter().any(|(id, meta)| {
					*id == candidate.node_id
						&& meta.node.as_ref().is_some_and(|cap| {
							cap.features
								.iter()
								.any(|feature| feature == paused_store::FEATURE)
						})
				})
		});
		if candidates.is_empty() {
			return Err(Unplaceable::NoCompatibleNode);
		}
		let compatible = |candidate: &Candidate| {
			members.iter().any(|(id, meta)| {
				*id == candidate.node_id
					&& meta.node.as_ref().is_some_and(|cap| {
						cap.arch == remote.arch
							&& cap.backend == remote.backend
							&& cap.cpu_baseline == remote.cpu_baseline
					})
			})
		};
		if let Some(index) = candidates
			.iter()
			.position(|c| c.node_id == owner && (cold_boot || compatible(c)))
		{
			return Ok((candidates.swap_remove(index), cold_boot));
		}
		if !cold_boot && candidates.iter().any(&compatible) {
			candidates.retain(compatible);
			return Ok((power_of_two(candidates, &mut rand::rng()), false));
		}
		if let Some(index) = candidates.iter().position(|c| c.node_id == owner) {
			return Ok((candidates.swap_remove(index), true));
		}
		Ok((power_of_two(candidates, &mut rand::rng()), true))
	}

	/// Filters, scores, and picks one node (power-of-two-choices among the top
	/// half).
	pub fn place<S: BuildHasher>(
		&self,
		members: &[(NodeId, MemberMeta)],
		statuses: &HashMap<NodeId, NodeStatus, S>,
		req: &PlacementRequest,
	) -> Result<Candidate, Unplaceable> {
		let counters = self.counters();
		let inflight = |id: NodeId| counters.get(&id).map_or(0, |c| c.inflight);
		let candidates = eligible(members, statuses, req, inflight)?;
		Ok(power_of_two(candidates, &mut rand::rng()))
	}
}

impl InflightGuard<'_> {
	/// Records the outcome and releases the in-flight slot.
	pub fn finish(mut self, outcome: CreateOutcome) {
		self.release(Some(outcome));
	}

	fn release(&mut self, outcome: Option<CreateOutcome>) {
		if self.done {
			return;
		}
		self.done = true;
		let mut counters = self.scheduler.counters.lock();
		let entry = counters.entry(self.node).or_default();
		entry.inflight = entry.inflight.saturating_sub(1);
		match outcome {
			Some(CreateOutcome::Success) => entry.successes += 1,
			Some(CreateOutcome::Failure) | None => entry.fails += 1,
		}
	}
}

impl Drop for InflightGuard<'_> {
	fn drop(&mut self) {
		self.release(None);
	}
}

/// Hard filters + score for every node member. Returns candidates sorted by
/// descending score, or why none qualified.
pub fn eligible<S: BuildHasher>(
	members: &[(NodeId, MemberMeta)],
	statuses: &HashMap<NodeId, NodeStatus, S>,
	req: &PlacementRequest,
	inflight: impl Fn(NodeId) -> u32,
) -> Result<Vec<Candidate>, Unplaceable> {
	let artifact_nodes: HashSet<NodeId> = req.artifacts.iter().map(|a| a.node_id).collect();
	let mut compatible = false;
	let mut out = Vec::new();
	for (id, meta) in members {
		let Some(cap) = meta.node.as_ref() else {
			continue;
		};
		if !meta.roles.node || req.excluded.contains(id) || req.pinned.is_some_and(|pin| pin != *id) {
			continue;
		}
		if cap.draining || statuses.get(id).copied().unwrap_or_default() != NodeStatus::Ready {
			continue;
		}
		if !req.arch.is_empty() && cap.arch != req.arch {
			continue;
		}
		if !req
			.features
			.iter()
			.all(|f| cap.features.iter().any(|have| have == f))
		{
			continue;
		}
		compatible = true;
		let vcpu_cap = u64::from(cap.cpus) * VCPU_OVERCOMMIT;
		let mem_cap = cap.memory_mb * MEMORY_OVERCOMMIT;
		let vcpu_after = u64::from(cap.vcpus_used) + u64::from(req.vcpus);
		let mem_after = cap.memory_used_mb + req.memory_mb;
		if vcpu_after > vcpu_cap || mem_after > mem_cap || cap.disk_free_mb < MIN_FREE_DISK_MB {
			continue;
		}
		let mut score = headroom(vcpu_after, vcpu_cap) + headroom(mem_after, mem_cap);
		if artifact_nodes.contains(id) {
			score += ARTIFACT_BONUS;
			let reusable = req.artifacts.iter().any(|a| {
				a.node_id == *id
					&& a.has_snapshot
					&& a.backend == cap.backend
					&& a.cpu_baseline == cap.cpu_baseline
			});
			if reusable {
				score += SNAPSHOT_BONUS;
			}
		}
		score = f64::from(inflight(*id)).mul_add(-INFLIGHT_PENALTY, score);
		out.push(Candidate { node_id: *id, cluster_url: meta.cluster_url.clone(), score });
	}
	if out.is_empty() {
		return Err(if compatible {
			Unplaceable::NoCapacity
		} else {
			Unplaceable::NoCompatibleNode
		});
	}
	out.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.node_id.cmp(&b.node_id)));
	Ok(out)
}

/// Free fraction after placing, in `[0, 1]`.
#[allow(clippy::cast_precision_loss, reason = "capacity figures are far below 2^52")]
fn headroom(after: u64, cap: u64) -> f64 {
	if cap == 0 {
		return 0.0;
	}
	1.0 - (after as f64 / cap as f64).min(1.0)
}

/// Picks two random candidates among the top half (at least two) and returns
/// the better one. `candidates` must be non-empty and sorted by score.
pub fn power_of_two(mut candidates: Vec<Candidate>, rng: &mut impl rand::Rng) -> Candidate {
	let pool = candidates.len().div_ceil(2).max(2).min(candidates.len());
	if pool == 1 {
		return candidates.swap_remove(0);
	}
	let a = rng.random_range(0..pool);
	let mut b = rng.random_range(0..pool - 1);
	if b >= a {
		b += 1;
	}
	// Lower index = higher score (sorted).
	candidates.swap_remove(a.min(b))
}

#[cfg(test)]
mod tests {
	use vmon_cluster::{NodeCapacity, Roles};

	use super::*;

	fn node(id: u64, f: impl FnOnce(&mut NodeCapacity)) -> (NodeId, MemberMeta) {
		let mut cap = NodeCapacity {
			arch: "aarch64".into(),
			backend: "hvf".into(),
			cpu_baseline: "m4".into(),
			cpus: 8,
			memory_mb: 16_384,
			disk_free_mb: 100_000,
			..NodeCapacity::default()
		};
		f(&mut cap);
		let meta = MemberMeta {
			roles: Roles { node: true, driver: false },
			cluster_url: format!("http://n{id}:7947"),
			node: Some(cap),
			..MemberMeta::default()
		};
		(NodeId(id), meta)
	}

	fn req() -> PlacementRequest {
		PlacementRequest {
			vcpus: 2,
			memory_mb: 1024,
			arch: "aarch64".into(),
			..PlacementRequest::default()
		}
	}

	fn ids(c: &[Candidate]) -> Vec<u64> {
		c.iter().map(|c| c.node_id.0).collect()
	}

	#[test]
	fn filters_status_draining_arch_and_features() {
		let members = vec![
			node(1, |_| {}),
			node(2, |c| c.draining = true),
			node(3, |c| c.arch = "x86_64".into()),
			node(4, |c| c.features = vec!["egress-proxy".into()]),
			node(5, |_| {}),
		];
		let statuses = HashMap::from([(NodeId(5), NodeStatus::Unhealthy)]);
		let got = eligible(&members, &statuses, &req(), |_| 0).unwrap();
		assert_eq!(ids(&got).len(), 2);
		assert!(ids(&got).contains(&1) && ids(&got).contains(&4));

		let mut needs_proxy = req();
		needs_proxy.features = vec!["egress-proxy"];
		assert_eq!(ids(&eligible(&members, &statuses, &needs_proxy, |_| 0).unwrap()), vec![4]);
	}

	#[test]
	fn distinguishes_incompatible_from_full() {
		let members = vec![node(1, |c| c.vcpus_used = 31)];
		assert_eq!(
			eligible(&members, &HashMap::new(), &req(), |_| 0).unwrap_err(),
			Unplaceable::NoCapacity
		);
		let mut x86 = req();
		x86.arch = "x86_64".into();
		assert_eq!(
			eligible(&members, &HashMap::new(), &x86, |_| 0).unwrap_err(),
			Unplaceable::NoCompatibleNode
		);
	}

	#[test]
	fn capacity_boundary_is_inclusive() {
		// 8 cpus * 4 = 32 vCPU cap; 30 used + 2 requested = 32 fits, 31 + 2 does
		// not.
		let fits = vec![node(1, |c| c.vcpus_used = 30)];
		assert!(eligible(&fits, &HashMap::new(), &req(), |_| 0).is_ok());
		let full = vec![node(1, |c| c.vcpus_used = 31)];
		assert!(eligible(&full, &HashMap::new(), &req(), |_| 0).is_err());
		// 16 GiB * 2 = 32768 MiB memory cap.
		let mem_full = vec![node(1, |c| c.memory_used_mb = 32_768 - 1023)];
		assert!(eligible(&mem_full, &HashMap::new(), &req(), |_| 0).is_err());
		let disk_low = vec![node(1, |c| c.disk_free_mb = MIN_FREE_DISK_MB - 1)];
		assert!(eligible(&disk_low, &HashMap::new(), &req(), |_| 0).is_err());
	}

	#[test]
	fn pin_and_exclusions_are_hard() {
		let members = vec![node(1, |_| {}), node(2, |_| {})];
		let mut pinned = req();
		pinned.pinned = Some(NodeId(2));
		assert_eq!(ids(&eligible(&members, &HashMap::new(), &pinned, |_| 0).unwrap()), vec![2]);
		pinned.excluded = vec![NodeId(2)];
		assert_eq!(
			eligible(&members, &HashMap::new(), &pinned, |_| 0).unwrap_err(),
			Unplaceable::NoCompatibleNode
		);
	}

	#[test]
	fn artifact_affinity_beats_slightly_more_headroom_but_not_inflight_pileup() {
		let members = vec![node(1, |c| c.vcpus_used = 4), node(2, |_| {})];
		let mut with_art = req();
		with_art.artifacts = vec![ArtifactLocation {
			node_id:      NodeId(1),
			backend:      "hvf".into(),
			cpu_baseline: "m4".into(),
			has_snapshot: true,
		}];
		let ranked = eligible(&members, &HashMap::new(), &with_art, |_| 0).unwrap();
		assert_eq!(ids(&ranked), vec![1, 2]);
		assert!(ranked[0].score - ranked[1].score > ARTIFACT_BONUS);
		// Seven in-flight creates (−1.75) outweigh the artifact + snapshot bonus.
		let ranked =
			eligible(&members, &HashMap::new(), &with_art, |id| if id == NodeId(1) { 7 } else { 0 })
				.unwrap();
		assert_eq!(ids(&ranked), vec![2, 1]);
	}

	#[test]
	fn paused_placement_prefers_owner_then_matching_snapshot_then_same_arch_cold_boot() {
		let scheduler = Scheduler::default();
		let remote = Remote {
			url:          "file:///tmp/paused".into(),
			manifest:     "paused/id/generation/manifest.json".into(),
			arch:         "aarch64".into(),
			backend:      "hvf".into(),
			cpu_baseline: "m4".into(),
			bytes:        1,
		};
		let members = vec![
			node(1, |c| c.vcpus_used = 20),
			node(2, |c| c.features.push(paused_store::FEATURE.into())),
			node(3, |c| {
				c.cpu_baseline = "m3".into();
				c.features.push(paused_store::FEATURE.into());
			}),
			node(4, |c| {
				c.backend = "kvm".into();
				c.features.push(paused_store::FEATURE.into());
			}),
			node(5, |c| c.arch = "x86_64".into()),
		];
		let place = |members: &[_], owner, request: &PlacementRequest| {
			scheduler.place_paused(members, &HashMap::new(), request, owner, &remote, false)
		};
		let (candidate, cold) = place(&members, NodeId(1), &req()).unwrap();
		assert_eq!(candidate.node_id, NodeId(1), "usable owner beats more headroom");
		let no_store = vec![node(6, |_| {})];
		assert_eq!(
			place(&no_store, NodeId(1), &req()).unwrap_err(),
			Unplaceable::NoCompatibleNode,
			"relocation requires configured object storage"
		);
		assert!(!cold);
		let (candidate, cold) = place(&members[1..], NodeId(1), &req()).unwrap();
		assert_eq!(candidate.node_id, NodeId(2), "compatible snapshot is preferred");
		assert!(!cold);
		let (candidate, cold) = place(&members[2..], NodeId(1), &req()).unwrap();
		assert!(matches!(candidate.node_id, NodeId(3 | 4)));
		assert!(cold, "backend/baseline mismatch must cold boot");
		let mut pinned = req();
		pinned.pinned = Some(NodeId(1));
		assert_eq!(
			place(&members[1..], NodeId(1), &pinned).unwrap_err(),
			Unplaceable::NoCompatibleNode,
			"a dead volume pin must not migrate"
		);
		assert_eq!(
			place(&members[4..], NodeId(1), &req()).unwrap_err(),
			Unplaceable::NoCompatibleNode,
			"different guest architectures cannot cold boot"
		);
	}

	#[test]
	fn paused_placement_skips_a_draining_or_unhealthy_owner() {
		let scheduler = Scheduler::default();
		let remote = Remote {
			url:          "s3://b/p".into(),
			manifest:     "paused/id/generation/manifest.json".into(),
			arch:         "aarch64".into(),
			backend:      "hvf".into(),
			cpu_baseline: "m4".into(),
			bytes:        1,
		};
		let other = |c: &mut NodeCapacity| c.features.push(paused_store::FEATURE.into());
		let members = vec![node(1, |c| c.draining = true), node(2, other)];
		let (candidate, cold) = scheduler
			.place_paused(&members, &HashMap::new(), &req(), NodeId(1), &remote, false)
			.unwrap();
		assert_eq!((candidate.node_id, cold), (NodeId(2), false), "gossip draining flag");
		let members = vec![node(1, |_| {}), node(2, other)];
		for status in [NodeStatus::Draining, NodeStatus::Unhealthy] {
			let statuses = HashMap::from([(NodeId(1), status)]);
			let (candidate, _) = scheduler
				.place_paused(&members, &statuses, &req(), NodeId(1), &remote, false)
				.unwrap();
			assert_eq!(candidate.node_id, NodeId(2), "{status:?} owner");
		}
	}

	#[test]
	fn snapshot_bonus_requires_matching_backend_and_baseline() {
		let members = vec![node(1, |c| c.cpu_baseline = "other".into()), node(2, |_| {})];
		let art = |id| ArtifactLocation {
			node_id:      NodeId(id),
			backend:      "hvf".into(),
			cpu_baseline: "m4".into(),
			has_snapshot: true,
		};
		let mut r = req();
		r.artifacts = vec![art(1), art(2)];
		let ranked = eligible(&members, &HashMap::new(), &r, |_| 0).unwrap();
		assert_eq!(ids(&ranked), vec![2, 1]);
		assert!((ranked[0].score - ranked[1].score - SNAPSHOT_BONUS).abs() < 1e-9);
	}

	#[test]
	fn power_of_two_never_picks_from_bottom_half() {
		let cands: Vec<Candidate> = (0..6)
			.map(|i| Candidate {
				node_id:     NodeId(i),
				cluster_url: String::new(),
				score:       10.0 - f64::from(i as u8),
			})
			.collect();
		let mut rng = rand::rng();
		let mut seen = HashSet::new();
		for _ in 0..500 {
			seen.insert(power_of_two(cands.clone(), &mut rng).node_id.0);
		}
		// Top half = {0,1,2}; the worst of the pool (2) can never win a pair.
		assert!(seen.iter().all(|id| *id < 2), "{seen:?}");
		assert!(seen.contains(&0) && seen.contains(&1));
		let single = vec![cands[3].clone()];
		assert_eq!(power_of_two(single, &mut rng).node_id, NodeId(3));
	}

	#[test]
	fn inflight_guard_counts_outcomes_and_drop_as_failure() {
		let s = Scheduler::default();
		let g = s.begin(NodeId(1));
		assert_eq!(s.node_counters(NodeId(1)).inflight, 1);
		g.finish(CreateOutcome::Success);
		drop(s.begin(NodeId(1)));
		assert_eq!(s.node_counters(NodeId(1)), NodeCounters {
			inflight:  0,
			successes: 1,
			fails:     1,
		});
	}
}
