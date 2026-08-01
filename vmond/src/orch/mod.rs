//! Horizontally-scalable sandbox orchestration ("v2" control plane).
//!
//! A fleet of stateless `vmon sched` servers schedules sandbox creates onto
//! `vmon serve` workers with no coordination and no datastore on the critical
//! path, in contrast to the strongly-consistent [`crate::mesh`] layer:
//!
//! ```text
//! SDK ──SandboxCreate──▶ vmon sched (any of N)
//!                          │  in-memory WorkerTable (fed from Redis stream)
//!                          │  pick worker, direct gRPC Create
//!                          ▼
//!                        vmon serve worker ── accept | reject(busy)
//!                          │ heartbeat: SET vmon:o:w:<wid> PX + XADD vmon:o:workers
//!                          ▼
//!                        Redis (worker state, sandbox routes; NOT in create path)
//! ```
//!
//! - **Workers are the source of truth.** Each worker publishes its own state:
//!   a self-expiring `vmon:o:w:<wid>` key (liveness = key exists) plus an
//!   append to the `vmon:o:workers` stream that schedulers follow into an
//!   in-memory [`table::WorkerTable`]. Heartbeats carry counters/capacity only,
//!   so their size is fixed at any fleet scale; per-sandbox routes are written
//!   by a dedicated route-writer task, batched via Redis pipelining on its own
//!   connection so a route backlog can never delay liveness.
//! - **Scheduling is load balancing.** Placement filters live workers and
//!   applies power-of-two-choices on reserved memory; a rejected or unreachable
//!   worker is penalized and the create retries elsewhere.
//! - **Redis is off the critical path.** Sandbox routes (`vmon:o:sb:<sid>`) are
//!   written asynchronously after a create is accepted; reads happen only for
//!   sandboxes this scheduler did not place itself.
//! - **Controller + autoscaler are leader-gated.** Every scheduler runs the
//!   loops; a `SET NX PX` lease (`vmon:o:leader`) picks the one that marks
//!   sandboxes of dead workers `lost` and drives HPA-like scale hooks.

pub mod autoscaler;
pub mod controller;
pub mod dashboard;
#[cfg(test)]
mod e2e_tests;
pub mod miniredis;
pub mod redis;
pub mod sched;
pub mod server;
pub mod table;
pub mod worker;

use serde::{Deserialize, Serialize};

/// Version tag carried by every heartbeat so incompatible future layouts can
/// be skipped instead of misparsed.
pub const WIRE_VERSION: u32 = 1;

/// Redis key/stream layout. Everything the orchestration layer stores lives
/// under `vmon:o:`; all values are compact JSON strings.
pub mod keys {
	/// Stream every worker heartbeat is appended to (`XADD`); schedulers
	/// `XREAD` it to keep their in-memory worker tables fresh.
	pub const WORKERS_STREAM: &str = "vmon:o:workers";
	/// Leader lease for the controller janitor and autoscaler singletons.
	pub const LEADER: &str = "vmon:o:leader";

	/// Self-expiring worker state key: `SET ... PX dead_after`. Key presence
	/// is liveness; schedulers bootstrap their tables by scanning this prefix.
	pub fn worker(wid: &str) -> String {
		format!("vmon:o:w:{wid}")
	}

	/// Sandbox route + state (`SandboxRoute` JSON), written asynchronously by
	/// schedulers on create and by workers on state changes.
	pub fn sandbox(sid: &str) -> String {
		format!("vmon:o:sb:{sid}")
	}

	/// Drain marker set by the autoscaler; a worker seeing its own drain key
	/// stops accepting new sandboxes.
	pub fn drain(wid: &str) -> String {
		format!("vmon:o:drain:{wid}")
	}

	/// Prefix of [`worker`] keys, for bootstrap scans.
	pub const WORKER_PREFIX: &str = "vmon:o:w:";
	/// Prefix of [`sandbox`] keys, for the controller's orphan sweep.
	pub const SANDBOX_PREFIX: &str = "vmon:o:sb:";
}

/// Hard capacity and current usage advertised by a worker.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Resources {
	/// Virtual CPUs (capacity) or vCPUs handed to running sandboxes (usage).
	pub vcpus:   u64,
	/// Guest memory in MiB.
	pub mem_mib: u64,
}

/// Periodic worker state snapshot: counters and capacity only, fixed size at
/// any fleet scale.
///
/// Serialized as compact JSON both into the worker's self-expiring key and onto
/// the workers stream. Per-sandbox state travels separately as [`SandboxRoute`]
/// writes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WorkerHeartbeat {
	/// Wire version ([`WIRE_VERSION`]); unknown versions are skipped.
	pub ver:          u32,
	/// Stable worker id.
	pub wid:          String,
	/// Advertised base URL peers and schedulers dial (`http://host:port`).
	pub url:          String,
	/// Normalized machine architecture (`aarch64` / `x86_64`).
	pub arch:         String,
	/// Hypervisor backend (`kvm` / `hvf`).
	pub backend:      String,
	/// Per-process monotonic heartbeat counter; restarts reset it, so
	/// ordering is decided by (`ts_ms`, `seq`).
	pub seq:          u64,
	/// Publish time, unix milliseconds.
	pub ts_ms:        u64,
	/// Advertised hard capacity.
	pub caps:         Resources,
	/// Resources currently handed to sandboxes.
	pub used:         Resources,
	/// Number of non-terminal sandboxes on the worker.
	pub running:      u64,
	/// Whether the worker admits new sandboxes right now.
	pub accepting:    bool,
	/// Whether the autoscaler marked this worker for drain.
	pub draining:     bool,
	/// Cumulative non-loopback host bytes received, when the platform
	/// exposes counters (`/proc/net/dev`); `None` elsewhere.
	#[serde(default)]
	pub net_rx_bytes: Option<u64>,
	/// Cumulative non-loopback host bytes transmitted; see `net_rx_bytes`.
	#[serde(default)]
	pub net_tx_bytes: Option<u64>,
}

impl WorkerHeartbeat {
	/// Whether `self` supersedes `previous` (later wall clock wins; equal
	/// clocks fall back to the per-process sequence).
	pub fn newer_than(&self, previous: &Self) -> bool {
		(self.ts_ms, self.seq) > (previous.ts_ms, previous.seq)
	}
}

/// Durable-ish sandbox route: which worker owns a sandbox and the last state
/// the control plane observed. Stored at [`keys::sandbox`] with a TTL so
/// terminal sandboxes age out on their own.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SandboxRoute {
	/// Sandbox name/id.
	pub sid:    String,
	/// Owning worker id.
	pub wid:    String,
	/// Owning worker base URL (the "tunnel URL" clients may dial directly).
	pub url:    String,
	/// Last observed lifecycle state; the controller rewrites this to
	/// `lost` when the owning worker dies.
	pub state:  String,
	/// Last update time, unix milliseconds.
	pub ts_ms:  u64,
	/// Final exit code, once known.
	#[serde(default, skip_serializing_if = "Option::is_none")]
	pub result: Option<i64>,
}

/// State the controller writes for sandboxes whose worker disappeared.
pub const STATE_LOST: &str = "lost";

/// TTL on sandbox route keys: long enough for humans and janitors to inspect
/// terminal sandboxes, short enough that Redis self-cleans without a GC.
pub const ROUTE_TTL_MS: u64 = 24 * 60 * 60 * 1000;

/// Whether an engine sandbox status occupies worker resources and dies with
/// its worker.
///
/// `suspended` is durable and survives; `stopped`/`terminated` are already
/// over; everything live is `running` or `paused`.
pub fn is_live_state(state: &str) -> bool {
	matches!(state, "running" | "paused")
}

/// Current unix time in milliseconds.
pub fn now_ms() -> u64 {
	use std::time::{SystemTime, UNIX_EPOCH};
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |elapsed| elapsed.as_millis() as u64)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn heartbeat_ordering_prefers_wall_clock_then_seq() {
		let base = WorkerHeartbeat {
			ver:          WIRE_VERSION,
			wid:          "w1".into(),
			url:          "http://a:1".into(),
			arch:         "aarch64".into(),
			backend:      "hvf".into(),
			seq:          10,
			ts_ms:        1_000,
			caps:         Resources { vcpus: 8, mem_mib: 16_384 },
			used:         Resources::default(),
			running:      0,
			accepting:    true,
			draining:     false,
			net_rx_bytes: None,
			net_tx_bytes: None,
		};
		let mut restarted = base.clone();
		restarted.seq = 1; // process restarted: seq reset ...
		restarted.ts_ms = 2_000; // ... but the clock moved on
		assert!(restarted.newer_than(&base));
		let mut same_tick = base.clone();
		same_tick.seq = 11;
		assert!(same_tick.newer_than(&base));
		assert!(!base.newer_than(&base));
	}

	#[test]
	fn wire_roundtrip_and_unknown_fields_tolerated() {
		// `sbs` is a legacy field (digests were removed from heartbeats);
		// old payloads carrying it must still decode, like any unknown field.
		let json = r#"{"ver":1,"wid":"w","url":"http://h:1","arch":"x86_64","backend":"kvm",
			"seq":1,"ts_ms":5,"caps":{"vcpus":1,"mem_mib":2},"used":{"vcpus":0,"mem_mib":0},
			"running":0,"accepting":true,"draining":false,
			"sbs":[{"sid":"sb-legacy","state":"running"}],"future_field":42}"#;
		let heartbeat: WorkerHeartbeat = serde_json::from_str(json).expect("tolerant decode");
		assert_eq!(heartbeat.wid, "w");
		let route = SandboxRoute {
			sid:    "sb-1".into(),
			wid:    "w".into(),
			url:    "http://h:1".into(),
			state:  "running".into(),
			ts_ms:  5,
			result: None,
		};
		let encoded = serde_json::to_string(&route).expect("encode");
		assert!(!encoded.contains("result"), "absent result must be omitted: {encoded}");
		assert_eq!(serde_json::from_str::<SandboxRoute>(&encoded).expect("decode"), route);
	}
}
