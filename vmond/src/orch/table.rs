//! In-memory worker view and placement for scheduler processes.
//!
//! The table is a cache of the latest [`WorkerHeartbeat`] per worker, fed by
//! the Redis stream follower (plus a bootstrap key scan). Placement is
//! deliberately load balancing, not bin packing: filter eligible workers,
//! then power-of-two-choices on reserved memory. Cross-scheduler races are
//! absorbed by worker-side admission (reject `busy` → retry elsewhere), so
//! nothing here needs coordination.

use std::{
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, Instant},
};

use dashmap::DashMap;
use serde_json::Value;

use super::WorkerHeartbeat;

/// Sizing knobs for liveness and forgetting.
#[derive(Clone, Copy, Debug)]
pub struct TableConfig {
	/// A worker whose last heartbeat is older than this is not placeable.
	pub dead_after: Duration,
	/// A worker unseen for this long is dropped from the table entirely
	/// (its last heartbeat is handed to the controller for lost-marking).
	pub reap_after: Duration,
}

impl TableConfig {
	/// Derive both windows from the worker-key TTL used on the Redis side.
	pub fn from_dead_after(dead_after: Duration) -> Self {
		Self { dead_after, reap_after: dead_after * 3 }
	}
}

struct WorkerEntry {
	heartbeat:        WorkerHeartbeat,
	seen:             Instant,
	penalty_until:    Option<Instant>,
	/// Memory reserved by creates this scheduler has in flight but that the
	/// worker's own `used` numbers cannot reflect yet.
	inflight_mem_mib: Arc<AtomicU64>,
}

impl WorkerEntry {
	fn live(&self, now: Instant, dead_after: Duration) -> bool {
		now.saturating_duration_since(self.seen) <= dead_after
	}

	fn penalized(&self, now: Instant) -> bool {
		self.penalty_until.is_some_and(|until| until > now)
	}

	/// Fraction of memory capacity already spoken for; >1 means overfull.
	fn load(&self, extra_mem_mib: u64) -> f64 {
		let caps = self.heartbeat.caps.mem_mib;
		if caps == 0 {
			return f64::INFINITY;
		}
		let reserved = self.heartbeat.used.mem_mib
			+ self.inflight_mem_mib.load(Ordering::Relaxed)
			+ extra_mem_mib;
		reserved as f64 / caps as f64
	}

	fn fits(&self, needs: &PlacementNeeds) -> bool {
		if let Some(arch) = &needs.arch
			&& !arch.eq_ignore_ascii_case(&self.heartbeat.arch)
		{
			return false;
		}
		// Memory is the hard resource; vCPUs oversubscribe like the engine
		// itself allows, so they only gate through capacity advertisement.
		self.load(needs.mem_mib) <= 1.0
	}
}

/// Resource shape of one create, extracted from the schemaless create spec.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlacementNeeds {
	/// Requested vCPUs (advisory; workers oversubscribe CPU).
	pub cpus:    u64,
	/// Requested guest memory in MiB (hard placement constraint).
	pub mem_mib: u64,
	/// Explicit architecture constraint, when the spec names one.
	pub arch:    Option<String>,
}

impl PlacementNeeds {
	/// Defaults mirroring [`crate::models::SandboxCreate`] serde defaults.
	pub fn from_create_json(spec: &Value) -> Self {
		let uint = |key: &str, fallback: u64| {
			spec
				.get(key)
				.and_then(Value::as_u64)
				.filter(|value| *value > 0)
				.unwrap_or(fallback)
		};
		let arch = spec
			.get("arch")
			.and_then(Value::as_str)
			.map(str::trim)
			.filter(|arch| !arch.is_empty())
			.map(str::to_owned);
		Self { cpus: uint("cpus", 1), mem_mib: uint("memory", 512), arch }
	}
}

/// One successful placement decision. Holds an in-flight memory reservation
/// on the chosen worker that releases on drop, so concurrent creates on this
/// scheduler spread out before heartbeats catch up.
pub struct Placement {
	/// Chosen worker id.
	pub wid:      String,
	/// Chosen worker base URL.
	pub url:      String,
	reserved_mib: u64,
	inflight:     Arc<AtomicU64>,
}

impl Drop for Placement {
	fn drop(&mut self) {
		self
			.inflight
			.fetch_sub(self.reserved_mib, Ordering::Relaxed);
	}
}

/// Latest-heartbeat-per-worker cache with placement.
pub struct WorkerTable {
	config:  TableConfig,
	workers: DashMap<String, WorkerEntry>,
}

impl WorkerTable {
	pub fn new(config: TableConfig) -> Self {
		Self { config, workers: DashMap::new() }
	}

	/// Merge one heartbeat; stale ones (per [`WorkerHeartbeat::newer_than`])
	/// are ignored so out-of-order stream delivery cannot roll state back.
	pub fn upsert(&self, heartbeat: WorkerHeartbeat) {
		if heartbeat.ver != super::WIRE_VERSION {
			return;
		}
		let now = Instant::now();
		match self.workers.entry(heartbeat.wid.clone()) {
			dashmap::Entry::Occupied(mut occupied) => {
				let entry = occupied.get_mut();
				if heartbeat.newer_than(&entry.heartbeat) {
					entry.heartbeat = heartbeat;
					entry.seen = now;
				}
			},
			dashmap::Entry::Vacant(vacant) => {
				vacant.insert(WorkerEntry {
					heartbeat,
					seen: now,
					penalty_until: None,
					inflight_mem_mib: Arc::new(AtomicU64::new(0)),
				});
			},
		}
	}

	/// Skip a worker for `duration` after it rejected or failed a create.
	pub fn penalize(&self, wid: &str, duration: Duration) {
		if let Some(mut entry) = self.workers.get_mut(wid) {
			entry.penalty_until = Some(Instant::now() + duration);
		}
	}

	/// Pick a worker for `needs`, excluding `exclude` (already-tried workers
	/// in a retry loop). Power-of-two-choices: two random eligible workers,
	/// lowest reserved-memory load wins.
	pub fn pick(&self, needs: &PlacementNeeds, exclude: &[String]) -> Option<Placement> {
		let now = Instant::now();
		// Snapshot the eligible set out of the shards; the in-flight counter
		// travels as an Arc so the reservation lands without a re-lookup.
		struct Candidate {
			wid:      String,
			url:      String,
			load:     f64,
			inflight: Arc<AtomicU64>,
		}
		let eligible: Vec<Candidate> = self
			.workers
			.iter()
			.filter(|entry| {
				entry.live(now, self.config.dead_after)
					&& entry.heartbeat.accepting
					&& !entry.heartbeat.draining
					&& !entry.penalized(now)
					&& !exclude.contains(&entry.heartbeat.wid)
					&& entry.fits(needs)
			})
			.map(|entry| Candidate {
				wid:      entry.heartbeat.wid.clone(),
				url:      entry.heartbeat.url.clone(),
				load:     entry.load(0),
				inflight: Arc::clone(&entry.inflight_mem_mib),
			})
			.collect();
		let chosen = match eligible.len() {
			0 => return None,
			1 => &eligible[0],
			count => {
				let first = &eligible[(rand::random::<u64>() % count as u64) as usize];
				let second = &eligible[(rand::random::<u64>() % count as u64) as usize];
				if first.load <= second.load {
					first
				} else {
					second
				}
			},
		};
		chosen.inflight.fetch_add(needs.mem_mib, Ordering::Relaxed);
		Some(Placement {
			wid:          chosen.wid.clone(),
			url:          chosen.url.clone(),
			reserved_mib: needs.mem_mib,
			inflight:     Arc::clone(&chosen.inflight),
		})
	}

	/// Latest heartbeats of live workers (List fan-out, roster, autoscaler).
	pub fn live(&self) -> Vec<WorkerHeartbeat> {
		let now = Instant::now();
		self
			.workers
			.iter()
			.filter(|entry| entry.live(now, self.config.dead_after))
			.map(|entry| entry.heartbeat.clone())
			.collect()
	}

	/// Base URL for a specific worker, live or not (sandbox routing prefers
	/// a possibly-stale URL over none: the dial itself will fail fast).
	pub fn url_of(&self, wid: &str) -> Option<String> {
		self
			.workers
			.get(wid)
			.map(|entry| entry.heartbeat.url.clone())
	}

	/// Drop workers unseen for `reap_after`, returning their final
	/// heartbeats so the controller can mark their sandboxes lost.
	pub fn reap(&self) -> Vec<WorkerHeartbeat> {
		let now = Instant::now();
		let dead: Vec<String> = self
			.workers
			.iter()
			.filter(|entry| now.saturating_duration_since(entry.seen) > self.config.reap_after)
			.map(|entry| entry.key().clone())
			.collect();
		dead
			.into_iter()
			.filter_map(|wid| {
				// `remove_if` re-checks staleness so a heartbeat racing this
				// sweep resurrects the worker instead of being dropped.
				self
					.workers
					.remove_if(&wid, |_wid, entry| {
						now.saturating_duration_since(entry.seen) > self.config.reap_after
					})
					.map(|(_wid, entry)| entry.heartbeat)
			})
			.collect()
	}

	/// Number of known (not necessarily live) workers.
	pub fn len(&self) -> usize {
		self.workers.len()
	}

	pub fn is_empty(&self) -> bool {
		self.workers.is_empty()
	}
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::{
		super::{Resources, WIRE_VERSION},
		*,
	};

	fn heartbeat(wid: &str, used_mem: u64) -> WorkerHeartbeat {
		WorkerHeartbeat {
			ver:          WIRE_VERSION,
			wid:          wid.to_owned(),
			url:          format!("http://{wid}:8000"),
			arch:         "aarch64".to_owned(),
			backend:      "hvf".to_owned(),
			seq:          1,
			ts_ms:        1,
			caps:         Resources { vcpus: 8, mem_mib: 8_192 },
			used:         Resources { vcpus: 0, mem_mib: used_mem },
			running:      0,
			accepting:    true,
			draining:     false,
			net_rx_bytes: None,
			net_tx_bytes: None,
		}
	}

	fn table() -> WorkerTable {
		WorkerTable::new(TableConfig::from_dead_after(Duration::from_secs(5)))
	}

	fn needs(mem_mib: u64) -> PlacementNeeds {
		PlacementNeeds { cpus: 1, mem_mib, arch: None }
	}

	#[test]
	fn needs_parse_defaults_and_overrides() {
		let defaults = PlacementNeeds::from_create_json(&json!({}));
		assert_eq!(defaults, needs(512));
		let explicit = PlacementNeeds::from_create_json(&json!({
			"cpus": 4, "memory": 2048, "arch": "x86_64", "image": "ubuntu"
		}));
		assert_eq!(explicit, PlacementNeeds {
			cpus:    4,
			mem_mib: 2048,
			arch:    Some("x86_64".to_owned()),
		});
	}

	#[test]
	fn pick_filters_capacity_arch_drain_and_exclusions() {
		let table = table();
		table.upsert(heartbeat("full", 8_192)); // no free memory
		let mut draining = heartbeat("draining", 0);
		draining.draining = true;
		table.upsert(draining);
		let mut wrong_arch = heartbeat("intel", 0);
		wrong_arch.arch = "x86_64".to_owned();
		table.upsert(wrong_arch);
		table.upsert(heartbeat("free", 0));

		for _attempt in 0..64 {
			let placement = table
				.pick(&PlacementNeeds { cpus: 1, mem_mib: 512, arch: Some("aarch64".to_owned()) }, &[])
				.expect("one eligible worker");
			assert_eq!(placement.wid, "free");
		}
		let constrained =
			PlacementNeeds { cpus: 1, mem_mib: 512, arch: Some("aarch64".to_owned()) };
		assert!(
			table.pick(&constrained, &["free".to_owned()]).is_none(),
			"excluding the only arch-compatible fit must yield no placement"
		);
		assert!(
			table.pick(&needs(16_384), &[]).is_none(),
			"a request larger than every capacity must not place"
		);
	}

	#[test]
	fn inflight_reservations_gate_and_release() {
		let table = table();
		table.upsert(heartbeat("only", 0));
		let first = table.pick(&needs(5_000), &[]).expect("first fits");
		assert!(
			table.pick(&needs(5_000), &[]).is_none(),
			"reservation must block a second oversized create before heartbeats update"
		);
		drop(first);
		assert!(table.pick(&needs(5_000), &[]).is_some(), "drop must release the reservation");
	}

	#[test]
	fn po2_prefers_less_loaded_and_penalties_stick() {
		let table = table();
		table.upsert(heartbeat("busy", 7_000));
		table.upsert(heartbeat("idle", 0));
		let mut idle_wins = 0;
		for _attempt in 0..64 {
			let placement = table.pick(&needs(256), &[]).expect("either fits");
			if placement.wid == "idle" {
				idle_wins += 1;
			}
		}
		// po2 picks the loaded worker only when both random probes hit it:
		// expected ~25% of draws; equality would sit at 50%.
		assert!(idle_wins > 32, "idle worker won only {idle_wins}/64 draws");

		table.penalize("idle", Duration::from_mins(1));
		for _attempt in 0..16 {
			assert_eq!(table.pick(&needs(256), &[]).expect("busy still fits").wid, "busy");
		}
	}

	#[test]
	fn stale_heartbeats_do_not_roll_back_and_reap_forgets() {
		let table = WorkerTable::new(TableConfig {
			dead_after: Duration::from_millis(10),
			reap_after: Duration::from_millis(10),
		});
		let mut fresh = heartbeat("w", 1_000);
		fresh.ts_ms = 10;
		table.upsert(fresh);
		let mut stale = heartbeat("w", 0);
		stale.ts_ms = 5;
		table.upsert(stale);
		assert_eq!(table.live()[0].used.mem_mib, 1_000, "older heartbeat must be ignored");

		std::thread::sleep(Duration::from_millis(30));
		assert!(table.live().is_empty(), "expired worker must not be live");
		let reaped = table.reap();
		assert_eq!(reaped.len(), 1);
		assert_eq!(reaped[0].wid, "w");
		assert!(table.is_empty());
	}
}
