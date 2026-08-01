//! HPA-like autoscaler: utilization policy, drain marking, scale hooks.
//!
//! Runs on the leader only (see [`super::controller::LeaderLease`]). The
//! policy is deliberately Kubernetes-HPA-shaped: aggregate memory utilization
//! across live workers, derive a desired replica count, and latch scale
//! decisions behind per-direction cooldowns. Actuation is delegated to shell
//! hooks (`scale_up_cmd` / `scale_down_cmd`) so the policy stays agnostic of
//! the machine provider; scale-down additionally marks victims with drain
//! keys so workers stop admitting before the provider reclaims them.

use std::{
	sync::Arc,
	time::{Duration, Instant},
};

use super::{WorkerHeartbeat, keys, redis::Redis, table::WorkerTable};
use crate::Result;

/// Hook processes get this long before they are killed and logged.
const HOOK_TIMEOUT: Duration = Duration::from_mins(1);

/// Autoscaler knobs. The policy only runs when `max > 0`.
#[derive(Clone, Debug)]
pub struct AutoscalerOptions {
	/// Lower clamp on the desired worker count.
	pub min:            u64,
	/// Upper clamp on the desired worker count; `0` disables the policy.
	pub max:            u64,
	/// Target aggregate memory utilization in `(0, 1]`; out-of-range values
	/// fall back to the default `0.7`.
	pub target_util:    f64,
	/// Tick cadence. The server owns the loop; stored here for reference.
	pub interval:       Duration,
	/// Minimum spacing between two scale-up decisions.
	pub cooldown_up:    Duration,
	/// Minimum spacing between two scale-down decisions.
	pub cooldown_down:  Duration,
	/// TTL on drain keys written for scale-down victims.
	pub drain_ttl:      Duration,
	/// `sh -c` hook run on scale-up.
	pub scale_up_cmd:   Option<String>,
	/// `sh -c` hook run on scale-down.
	pub scale_down_cmd: Option<String>,
}

impl Default for AutoscalerOptions {
	fn default() -> Self {
		Self {
			min:            0,
			max:            0,
			target_util:    0.7,
			interval:       Duration::from_secs(15),
			cooldown_up:    Duration::from_secs(30),
			cooldown_down:  Duration::from_mins(5),
			drain_ttl:      Duration::from_mins(15),
			scale_up_cmd:   None,
			scale_down_cmd: None,
		}
	}
}

/// One tick's decision, computed purely from the worker table and cooldowns.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Plan {
	/// Nothing to do (on target, delta zero, or inside a cooldown window).
	Hold,
	/// Grow the fleet to `desired` (`delta` = new workers to add).
	Up { desired: u64, delta: u64 },
	/// Shrink the fleet to `desired`, draining exactly the listed workers.
	Down { desired: u64, drain: Vec<String> },
}

/// Pure HPA replica math.
///
/// With `util = used_mem_mib / caps_mem_mib` over all live workers, the
/// desired count is `ceil(live * util / target_util)` — the smallest fleet
/// that brings utilization back to `target_util` assuming homogeneous
/// workers — clamped to `[min, max]`. With no live workers or no advertised
/// capacity there is no signal, so the floor `max(min, 1)` (still clamped to
/// `max`) keeps at least one worker coming.
pub fn desired_workers(
	live: usize,
	used_mem_mib: u64,
	caps_mem_mib: u64,
	options: &AutoscalerOptions,
) -> u64 {
	let max = options.max.max(1);
	if live == 0 || caps_mem_mib == 0 {
		return options.min.max(1).min(max);
	}
	let target = if options.target_util > 0.0 && options.target_util <= 1.0 {
		options.target_util
	} else {
		0.7
	};
	#[allow(clippy::cast_precision_loss, reason = "worker counts and MiB totals are far below 2^52")]
	let raw = (live as f64) * (used_mem_mib as f64 / caps_mem_mib as f64) / target;
	#[allow(
		clippy::cast_possible_truncation,
		clippy::cast_sign_loss,
		reason = "raw is non-negative and clamped to max before the fleet could overflow"
	)]
	let desired = raw.ceil().max(0.0).min(max as f64) as u64;
	desired.clamp(options.min.min(max), max)
}

/// Leader-gated scaling loop state.
pub struct Autoscaler {
	options:   AutoscalerOptions,
	redis:     Redis,
	table:     Arc<WorkerTable>,
	last_up:   Option<Instant>,
	last_down: Option<Instant>,
}

impl Autoscaler {
	pub const fn new(options: AutoscalerOptions, redis: Redis, table: Arc<WorkerTable>) -> Self {
		Self { options, redis, table, last_up: None, last_down: None }
	}

	/// The configured policy (the server loop reads `interval`).
	pub const fn options(&self) -> &AutoscalerOptions {
		&self.options
	}

	/// Pure decision for `now`: aggregate live utilization, run
	/// [`desired_workers`], and gate each direction behind its cooldown.
	/// Draining workers still count toward `live` (they hold resources until
	/// gone) and sort first for re-draining, which refreshes their drain TTL
	/// instead of over-draining fresh workers.
	pub fn plan(&self, now: Instant) -> Plan {
		if self.options.max == 0 {
			return Plan::Hold;
		}
		let workers = self.table.live();
		let live = workers.len();
		let used: u64 = workers.iter().map(|hb| hb.used.mem_mib).sum();
		let caps: u64 = workers.iter().map(|hb| hb.caps.mem_mib).sum();
		let desired = desired_workers(live, used, caps, &self.options);
		let live_count = u64::try_from(live).unwrap_or(u64::MAX);
		match desired.cmp(&live_count) {
			std::cmp::Ordering::Greater if cooled(self.last_up, self.options.cooldown_up, now) => {
				Plan::Up { desired, delta: desired - live_count }
			},
			std::cmp::Ordering::Less if cooled(self.last_down, self.options.cooldown_down, now) => {
				Plan::Down { desired, drain: pick_drain(workers, live_count - desired) }
			},
			_ => Plan::Hold,
		}
	}

	/// Decide and act: latch the cooldown for the chosen direction, mark
	/// scale-down victims with drain keys, and run the configured hook.
	/// Hook and per-key Redis failures are logged, never propagated — the
	/// policy loop must survive a flaky provider script.
	pub async fn tick(&mut self) -> Result<()> {
		let now = Instant::now();
		let plan = self.plan(now);
		let live = self.table.live().len();
		match plan {
			Plan::Hold => {},
			Plan::Up { desired, delta } => {
				self.last_up = Some(now);
				#[allow(
					clippy::cast_possible_wrap,
					reason = "delta <= max, a configured fleet size nowhere near i64::MAX"
				)]
				let signed = delta as i64;
				if let Some(cmd) = &self.options.scale_up_cmd {
					run_hook(cmd, desired, signed, live, None, None).await;
				} else {
					tracing::info!(desired, delta, live, "scale up (no hook configured)");
				}
			},
			Plan::Down { desired, drain } => {
				self.last_down = Some(now);
				let ttl_ms = u64::try_from(self.options.drain_ttl.as_millis())
					.unwrap_or(u64::MAX)
					.max(1);
				for wid in &drain {
					if let Err(error) = self.redis.set_px(&keys::drain(wid), b"1", ttl_ms).await {
						tracing::warn!(%wid, %error, "drain key write failed");
					}
				}
				#[allow(
					clippy::cast_possible_wrap,
					reason = "drain length is bounded by the live fleet size"
				)]
				let signed = -(drain.len() as i64);
				let wids = drain.join(" ");
				// Workers that are already drained empty (this pick or an
				// earlier one) are safe to terminate right now; provider
				// hooks act on these and leave the rest to a later tick.
				let mut idle: Vec<String> = self
					.table
					.live()
					.into_iter()
					.filter(|hb| hb.running == 0 && (hb.draining || drain.contains(&hb.wid)))
					.map(|hb| hb.wid)
					.collect();
				idle.sort_unstable();
				let idle = idle.join(" ");
				match &self.options.scale_down_cmd {
					Some(cmd) => run_hook(cmd, desired, signed, live, Some(&wids), Some(&idle)).await,
					None => {
						tracing::info!(desired, live, drain = %wids, idle = %idle, "scale down (no hook configured)");
					},
				}
			},
		}
		Ok(())
	}
}

/// Whether a direction's cooldown window has fully elapsed (or never latched).
fn cooled(last: Option<Instant>, cooldown: Duration, now: Instant) -> bool {
	last.is_none_or(|at| now.saturating_duration_since(at) >= cooldown)
}

/// Pick `count` drain victims: fewest running sandboxes first (cheapest to
/// evacuate), wid as a deterministic tie-break. Never returns more than
/// `count`, so the fleet is never drained below the desired size.
fn pick_drain(mut workers: Vec<WorkerHeartbeat>, count: u64) -> Vec<String> {
	workers.sort_by(|a, b| (a.running, &a.wid).cmp(&(b.running, &b.wid)));
	workers
		.into_iter()
		.take(usize::try_from(count).unwrap_or(usize::MAX))
		.map(|hb| hb.wid)
		.collect()
}

/// Run one `sh -c` hook with the scaling env contract:
/// `VMON_SCALE_DESIRED`, `VMON_SCALE_DELTA` (signed), `VMON_SCALE_LIVE`, and
/// for scale-down `VMON_DRAIN_WIDS` (all victims, space-separated) plus
/// `VMON_IDLE_WIDS` (the drained-and-empty subset a provider hook may
/// terminate immediately). Failures and timeouts are logged; they never fail
/// the policy loop.
async fn run_hook(
	cmd: &str,
	desired: u64,
	delta: i64,
	live: usize,
	drain_wids: Option<&str>,
	idle_wids: Option<&str>,
) {
	let mut command = tokio::process::Command::new("sh");
	command
		.arg("-c")
		.arg(cmd)
		.env("VMON_SCALE_DESIRED", desired.to_string())
		.env("VMON_SCALE_DELTA", delta.to_string())
		.env("VMON_SCALE_LIVE", live.to_string())
		.kill_on_drop(true);
	if let Some(wids) = drain_wids {
		command.env("VMON_DRAIN_WIDS", wids);
	}
	if let Some(wids) = idle_wids {
		command.env("VMON_IDLE_WIDS", wids);
	}
	let mut child = match command.spawn() {
		Ok(child) => child,
		Err(error) => {
			tracing::warn!(%cmd, %error, "scale hook failed to spawn");
			return;
		},
	};
	match tokio::time::timeout(HOOK_TIMEOUT, child.wait()).await {
		Ok(Ok(status)) if status.success() => {},
		Ok(Ok(status)) => tracing::warn!(%cmd, %status, "scale hook exited non-zero"),
		Ok(Err(error)) => tracing::warn!(%cmd, %error, "scale hook wait failed"),
		Err(_elapsed) => {
			tracing::warn!(%cmd, timeout = ?HOOK_TIMEOUT, "scale hook timed out; killing");
			if let Err(error) = child.start_kill() {
				tracing::warn!(%cmd, %error, "scale hook kill failed");
			}
		},
	}
}

#[cfg(test)]
mod tests {
	use super::{
		super::{Resources, WIRE_VERSION, miniredis::MiniRedis, table::TableConfig},
		*,
	};

	fn heartbeat(wid: &str, used_mem: u64, running: u64) -> WorkerHeartbeat {
		WorkerHeartbeat {
			ver: WIRE_VERSION,
			wid: wid.to_owned(),
			url: format!("http://{wid}:8000"),
			arch: "aarch64".to_owned(),
			backend: "hvf".to_owned(),
			seq: 1,
			ts_ms: 1,
			caps: Resources { vcpus: 8, mem_mib: 1_000 },
			used: Resources { vcpus: 0, mem_mib: used_mem },
			running,
			accepting: true,
			draining: false,
			net_rx_bytes: None,
			net_tx_bytes: None,
		}
	}

	fn table() -> Arc<WorkerTable> {
		Arc::new(WorkerTable::new(TableConfig::from_dead_after(Duration::from_secs(5))))
	}

	fn options(min: u64, max: u64) -> AutoscalerOptions {
		AutoscalerOptions { min, max, ..AutoscalerOptions::default() }
	}

	#[test]
	fn desired_workers_math() {
		// No signal → floor of max(min, 1), clamped to max.
		assert_eq!(desired_workers(0, 0, 0, &options(2, 10)), 2);
		assert_eq!(desired_workers(0, 0, 0, &options(0, 10)), 1);
		assert_eq!(desired_workers(3, 500, 0, &options(0, 2)), 1, "caps==0 is also no signal");
		// Under target (util 0.1, target 0.7): ceil(4 * 0.1 / 0.7) = 1.
		assert_eq!(desired_workers(4, 1_000, 10_000, &options(1, 10)), 1);
		// Over target (util 0.9): ceil(2 * 0.9 / 0.7) = 3.
		assert_eq!(desired_workers(2, 9_000, 10_000, &options(1, 10)), 3);
		// Clamps.
		assert_eq!(desired_workers(2, 9_000, 10_000, &options(1, 2)), 2, "max clamps growth");
		assert_eq!(desired_workers(4, 0, 4_000, &options(3, 10)), 3, "min clamps shrink");
		// Scale to zero is allowed when min == 0.
		assert_eq!(desired_workers(2, 0, 2_000, &options(0, 10)), 0);
	}

	#[tokio::test]
	async fn plan_latches_up_cooldown() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let table = table();
		// util 0.9 over 2 workers → desired 3 → Up { delta: 1 }.
		table.upsert(heartbeat("a", 900, 1));
		table.upsert(heartbeat("b", 900, 1));
		let mut scaler = Autoscaler::new(options(1, 10), redis, Arc::clone(&table));

		let now = Instant::now();
		assert_eq!(scaler.plan(now), Plan::Up { desired: 3, delta: 1 });

		scaler.last_up = now.checked_sub(Duration::from_secs(1));
		assert_eq!(scaler.plan(now), Plan::Hold, "inside cooldown_up must hold");
		scaler.last_up = now.checked_sub(Duration::from_secs(31));
		assert_eq!(scaler.plan(now), Plan::Up { desired: 3, delta: 1 }, "cooldown elapsed");
	}

	#[tokio::test]
	async fn plan_drains_emptiest_and_latches_down_cooldown() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let table = table();
		// util 0.3 over 4 workers, target 0.7 → desired ceil(1.714) = 2.
		table.upsert(heartbeat("busy", 300, 5));
		table.upsert(heartbeat("idle", 300, 0));
		table.upsert(heartbeat("mid", 300, 2));
		table.upsert(heartbeat("low", 300, 1));
		let mut scaler = Autoscaler::new(options(1, 10), redis, Arc::clone(&table));

		let now = Instant::now();
		let Plan::Down { desired, drain } = scaler.plan(now) else {
			panic!("expected a Down plan");
		};
		assert_eq!(desired, 2);
		assert_eq!(drain, vec!["idle".to_owned(), "low".to_owned()], "emptiest workers drain");

		scaler.last_down = now.checked_sub(Duration::from_mins(1));
		assert_eq!(scaler.plan(now), Plan::Hold, "inside cooldown_down must hold");
		scaler.last_down = now.checked_sub(Duration::from_secs(301));
		assert!(matches!(scaler.plan(now), Plan::Down { .. }), "cooldown elapsed");
	}

	#[tokio::test]
	async fn tick_down_sets_drain_keys_and_env_contract() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");
		let table = table();
		// util 0 over 3 workers → desired = min = 1 → drain the 2 emptiest.
		table.upsert(heartbeat("full", 0, 3));
		table.upsert(heartbeat("one", 0, 1));
		table.upsert(heartbeat("zero", 0, 0));

		let dir = tempfile::tempdir().expect("tempdir");
		let marker = dir.path().join("marker");
		let mut opts = options(1, 10);
		opts.scale_down_cmd = Some(format!(
			"printf '%s %s %s %s|%s' \"$VMON_SCALE_DESIRED\" \"$VMON_SCALE_DELTA\" \
			 \"$VMON_SCALE_LIVE\" \"$VMON_DRAIN_WIDS\" \"$VMON_IDLE_WIDS\" > {}",
			marker.display()
		));
		let mut scaler = Autoscaler::new(opts, redis.clone(), Arc::clone(&table));
		scaler.tick().await.expect("tick");

		let written = std::fs::read_to_string(&marker).expect("hook must write the marker");
		// Drain victims: the two emptiest ("zero", "one"); immediately
		// terminable ($VMON_IDLE_WIDS): only "zero", which is already empty.
		assert_eq!(written, "1 -2 3 zero one|zero");
		assert!(scaler.last_down.is_some(), "down cooldown must latch");
		for wid in ["zero", "one"] {
			let value = redis.get(&keys::drain(wid)).await.expect("get");
			assert_eq!(value.as_deref(), Some(b"1".as_slice()), "drain key for {wid}");
		}
		assert!(
			redis
				.get(&keys::drain("full"))
				.await
				.expect("get")
				.is_none(),
			"surviving worker must not be drained"
		);
	}
}
