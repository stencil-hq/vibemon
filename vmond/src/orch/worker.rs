//! Worker-side orchestration: liveness heartbeat, route writer, and create
//! admission.
//!
//! [`OrchWorker`] runs two independent tasks:
//!
//! - The **liveness task** publishes a counters-only [`WorkerHeartbeat`] on a
//!   fixed cadence: refresh the drain flag, `SET PX` the self-expiring worker
//!   key, `XADD` the workers stream, and enqueue owed route changes. The
//!   payload is fixed-size at any fleet scale (a worker with 6144 sandboxes
//!   publishes the same tiny heartbeat as an idle one).
//! - The **route writer task** drains the coalescing route-change queue in
//!   batches of [`ROUTE_BATCH_MAX`] and writes them with Redis pipelining.
//!
//! Liveness invariant: a route backlog is structurally incapable of delaying
//! a liveness publish. The two tasks share only the in-memory queue, and each
//! owns a separate [`Redis`] handle (commands serialize per handle behind a
//! mutex), so a large route pipeline can never block the liveness `SET`.
//!
//! [`OrchWorker`] also gates sandbox admission so schedulers can rely on
//! accept/reject semantics: an [`AdmitGuard`] reserves one inflight slot for
//! the duration of a create dispatch.

#[cfg(target_os = "linux")]
use std::io::Read as _;
#[cfg(target_os = "macos")]
use std::mem::MaybeUninit;
use std::{
	collections::HashSet,
	io,
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	time::Duration,
};
#[cfg(target_os = "macos")]
unsafe extern "C" {
	fn mach_host_self() -> libc::mach_port_t;
}

use dashmap::DashMap;
use serde_json::Value;
use tokio::sync::Notify;

use super::{
	ROUTE_TTL_MS, Resources, SandboxRoute, WIRE_VERSION, WorkerHeartbeat, is_live_state, keys,
	now_ms, redis::Redis,
};
use crate::{EngineError, Result, engine::EngineApi, home::Home};

/// Approximate bound on the workers stream (`XADD MAXLEN ~`).
const STREAM_MAXLEN: u64 = 8192;
/// Maximum route writes drained into one pipelined Redis round.
const ROUTE_BATCH_MAX: usize = 512;
/// Pause before the route writer retries a failed pipeline.
const ROUTE_RETRY_DELAY: Duration = Duration::from_millis(500);

/// Static identity and tuning for one orchestration worker.
pub struct OrchWorkerOptions {
	/// `redis://` endpoint of the orchestration datastore.
	pub redis_url:          String,
	/// Stable worker id.
	pub wid:                String,
	/// Advertised base URL schedulers dial.
	pub url:                String,
	/// Normalized host architecture (`aarch64` / `x86_64`).
	pub arch:               String,
	/// Hypervisor backend (`kvm` / `hvf`).
	pub backend:            String,
	/// Advertised hard capacity.
	pub caps:               Resources,
	/// Heartbeat publish cadence.
	pub heartbeat:          Duration,
	/// TTL on the self-expiring worker key; missing key means dead.
	pub dead_after:         Duration,
	/// Optional operator safety ceiling; zero lets measured memory govern
	/// admission.
	pub max_sandboxes:      u64,
	/// Available memory reserved so `CoW` divergence cannot starve host
	/// services.
	pub memory_reserve_mib: u64,
}

/// Reservation of one admission slot; dropping it releases the slot.
pub struct AdmitGuard {
	worker: Arc<OrchWorker>,
}

impl std::fmt::Debug for AdmitGuard {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("AdmitGuard").finish_non_exhaustive()
	}
}

impl Drop for AdmitGuard {
	fn drop(&mut self) {
		self.worker.inflight.fetch_sub(1, Ordering::SeqCst);
	}
}

/// Last published lifecycle facts for one sandbox.
struct Tracked {
	state:  String,
	result: Option<i64>,
}

/// Latest owed route state for one sandbox, keyed by sid in the route queue.
struct RouteChange {
	state:  String,
	result: Option<i64>,
}

/// Aborts the wrapped task on drop; ties the route writer's lifetime to the
/// liveness task.
struct AbortOnDrop(tokio::task::JoinHandle<()>);

impl Drop for AbortOnDrop {
	fn drop(&mut self) {
		self.0.abort();
	}
}

/// Worker-side publisher + admission gate.
pub struct OrchWorker {
	options:      OrchWorkerOptions,
	/// Liveness connection: heartbeat `SET PX` / `XADD` / drain `GET` only.
	redis:        Redis,
	/// Route writer connection; big pipelines must never queue behind (or
	/// ahead of) a liveness command, so the writer owns its own handle.
	route_redis:  Redis,
	seq:          AtomicU64,
	inflight:     AtomicU64,
	draining:     AtomicBool,
	/// Last published sandbox facts by sid, for route change detection and
	/// the admission running count.
	last_states:  DashMap<String, Tracked>,
	/// Owed route writes, coalesced per sid (latest state wins). Bounded by
	/// the sandbox count, so backlog can never grow without bound.
	route_queue:  DashMap<String, RouteChange>,
	/// Wakes the route writer after `route_queue` gains entries.
	route_notify: Notify,
}

impl OrchWorker {
	/// Build a worker handle; no Redis I/O happens until the first publish.
	pub fn new(options: OrchWorkerOptions) -> Result<Arc<Self>> {
		let redis = Redis::new(&options.redis_url)?;
		let route_redis = Redis::new(&options.redis_url)?;
		Ok(Arc::new(Self {
			options,
			redis,
			route_redis,
			seq: AtomicU64::new(0),
			inflight: AtomicU64::new(0),
			draining: AtomicBool::new(false),
			last_states: DashMap::new(),
			route_queue: DashMap::new(),
			route_notify: Notify::new(),
		}))
	}

	/// Stable worker id.
	pub fn wid(&self) -> &str {
		&self.options.wid
	}

	/// Advertised base URL.
	pub fn url(&self) -> &str {
		&self.options.url
	}

	/// Reserve one admission slot for an incoming sandbox create.
	///
	/// Rejects with [`EngineError::busy`] while draining, when actual host
	/// available memory has reached the configured reserve, or when the
	/// optional hard count ceiling (last published running count plus inflight
	/// creates) is exhausted. The guard owns the worker, so it may outlive the
	/// RPC that reserved it (background `no_wait` boots).
	pub fn admit(self: &Arc<Self>) -> Result<AdmitGuard> {
		let available_mib = host_available_memory_mib().map_err(|error| {
			EngineError::busy(format!("worker memory headroom unavailable: {error}"))
		})?;
		self.admit_with_available(available_mib)
	}

	fn admit_with_available(self: &Arc<Self>, available_mib: u64) -> Result<AdmitGuard> {
		if self.draining.load(Ordering::SeqCst) {
			return Err(EngineError::busy("worker is draining"));
		}
		let reserved = self.inflight.fetch_add(1, Ordering::SeqCst);
		let max = self.options.max_sandboxes;
		if max > 0 && self.live_count() + reserved >= max {
			self.inflight.fetch_sub(1, Ordering::SeqCst);
			return Err(EngineError::busy(format!("worker at capacity ({max} sandboxes)")));
		}
		let reserve_mib = self.options.memory_reserve_mib;
		if available_mib <= reserve_mib {
			self.inflight.fetch_sub(1, Ordering::SeqCst);
			return Err(EngineError::busy(format!(
				"worker memory reserve reached ({available_mib} MiB available, {reserve_mib} MiB \
				 reserved)"
			)));
		}
		Ok(AdmitGuard { worker: Arc::clone(self) })
	}

	/// Number of live sandboxes in the last published snapshot.
	fn live_count(&self) -> u64 {
		self
			.last_states
			.iter()
			.filter(|entry| is_live_state(&entry.value().state))
			.count() as u64
	}

	/// Derive one counters-only heartbeat from engine sandbox views and actual
	/// host memory pressure. No Redis I/O happens; only the sequence advances.
	pub fn build_heartbeat(&self, views: &[Value]) -> WorkerHeartbeat {
		self.build_heartbeat_with_available(views, host_available_memory_mib().ok())
	}

	fn build_heartbeat_with_available(
		&self,
		views: &[Value],
		available_memory_mib: Option<u64>,
	) -> WorkerHeartbeat {
		let mut running = 0_u64;
		let mut used = Resources::default();
		for view in views {
			if is_live_state(view_state(view)) {
				running += 1;
				used.vcpus += view.get("cpus").and_then(Value::as_u64).unwrap_or(0);
			}
		}
		used.mem_mib = available_memory_mib.map_or(self.options.caps.mem_mib, |available| {
			self.options.caps.mem_mib.saturating_sub(available)
		});
		let draining = self.draining.load(Ordering::SeqCst);
		let inflight = self.inflight.load(Ordering::SeqCst);
		let max = self.options.max_sandboxes;
		let below_count_ceiling = max == 0 || running + inflight < max;
		let has_memory_headroom =
			available_memory_mib.is_some_and(|available| available > self.options.memory_reserve_mib);
		let accepting = !draining && below_count_ceiling && has_memory_headroom;
		let (net_rx_bytes, net_tx_bytes) = match crate::net::host_network_bytes() {
			Some((rx, tx)) => (Some(rx), Some(tx)),
			None => (None, None),
		};
		WorkerHeartbeat {
			ver: WIRE_VERSION,
			wid: self.options.wid.clone(),
			url: self.options.url.clone(),
			arch: self.options.arch.clone(),
			backend: self.options.backend.clone(),
			seq: self.seq.fetch_add(1, Ordering::SeqCst) + 1,
			ts_ms: now_ms(),
			caps: self.options.caps,
			used,
			running,
			accepting,
			draining,
			net_rx_bytes,
			net_tx_bytes,
		}
	}

	/// Publish one liveness heartbeat: refresh the drain flag, `SET` the
	/// self-expiring worker key, append to the workers stream, then enqueue a
	/// route change for every sandbox whose state changed since the previous
	/// publish (a sandbox that vanished from the engine counts as changed to
	/// `terminated`). Route writes themselves happen on the route writer
	/// task; nothing here waits on them.
	pub async fn publish_once(&self, views: &[Value]) -> Result<()> {
		let drained = self.redis.get(&keys::drain(&self.options.wid)).await?;
		self.draining.store(drained.is_some(), Ordering::SeqCst);
		let heartbeat = self.build_heartbeat(views);
		let payload = serde_json::to_vec(&heartbeat)?;
		let dead_after_ms = self.options.dead_after.as_millis() as u64;
		self
			.redis
			.set_px(&keys::worker(&self.options.wid), &payload, dead_after_ms)
			.await?;
		self
			.redis
			.xadd(keys::WORKERS_STREAM, STREAM_MAXLEN, &payload)
			.await?;
		self.enqueue_route_changes(views);
		Ok(())
	}

	/// Fold the current views into the tracked snapshot and enqueue every
	/// owed route write: new or state-changed sandboxes, plus sandboxes that
	/// disappeared from the engine (reported as `terminated`, keeping the
	/// last known exit code). The queue coalesces per sid — the latest state
	/// wins — so its size is bounded by the sandbox count.
	fn enqueue_route_changes(&self, views: &[Value]) {
		let mut changed = false;
		let mut seen = HashSet::with_capacity(views.len());
		for view in views {
			let sid = view_sid(view);
			let state = view_state(view);
			let result = view.get("returncode").and_then(Value::as_i64);
			seen.insert(sid.to_owned());
			let is_new_state = self
				.last_states
				.get(sid)
				.is_none_or(|tracked| tracked.state != state);
			self
				.last_states
				.insert(sid.to_owned(), Tracked { state: state.to_owned(), result });
			if is_new_state {
				self
					.route_queue
					.insert(sid.to_owned(), RouteChange { state: state.to_owned(), result });
				changed = true;
			}
		}
		self.last_states.retain(|sid, tracked| {
			if seen.contains(sid) {
				return true;
			}
			self.route_queue.insert(sid.clone(), RouteChange {
				state:  "terminated".to_owned(),
				result: tracked.result,
			});
			changed = true;
			false
		});
		if changed {
			self.route_notify.notify_one();
		}
	}

	/// Drain up to [`ROUTE_BATCH_MAX`] queued route changes and write them in
	/// one pipelined round on the route connection; returns how many routes
	/// were written. On failure the drained entries are re-queued, unless a
	/// newer change for the same sandbox arrived meanwhile (latest wins).
	async fn flush_route_batch(&self) -> Result<usize> {
		let sids: Vec<String> = self
			.route_queue
			.iter()
			.take(ROUTE_BATCH_MAX)
			.map(|entry| entry.key().clone())
			.collect();
		if sids.is_empty() {
			return Ok(0);
		}
		let ts_ms = now_ms();
		let mut entries = Vec::with_capacity(sids.len());
		let mut drained = Vec::with_capacity(sids.len());
		for sid in sids {
			// `remove` yields the latest queued change, so coalescing keeps
			// working even while a drain is in flight.
			let Some((sid, change)) = self.route_queue.remove(&sid) else {
				continue;
			};
			let route = SandboxRoute {
				sid: sid.clone(),
				wid: self.options.wid.clone(),
				url: self.options.url.clone(),
				state: change.state.clone(),
				ts_ms,
				result: change.result,
			};
			match serde_json::to_vec(&route) {
				Ok(payload) => {
					entries.push((keys::sandbox(&sid), payload));
					drained.push((sid, change));
				},
				Err(error) => {
					drained.push((sid, change));
					self.requeue(drained);
					return Err(error.into());
				},
			}
		}
		if entries.is_empty() {
			return Ok(0);
		}
		if let Err(error) = self
			.route_redis
			.set_px_pipeline(&entries, ROUTE_TTL_MS)
			.await
		{
			self.requeue(drained);
			return Err(error);
		}
		Ok(entries.len())
	}

	/// Put drained changes back without clobbering newer queued states.
	fn requeue(&self, drained: Vec<(String, RouteChange)>) {
		for (sid, change) in drained {
			self.route_queue.entry(sid).or_insert(change);
		}
		self.route_notify.notify_one();
	}

	/// Spawn the route writer: wait for queued changes, then drain them in
	/// pipelined batches until the queue is empty. Failures re-queue the
	/// batch, log, and retry after [`ROUTE_RETRY_DELAY`]; the liveness task
	/// is never involved.
	pub fn spawn_route_writer(self: &Arc<Self>) -> tokio::task::JoinHandle<()> {
		let worker = Arc::clone(self);
		tokio::spawn(async move {
			loop {
				worker.route_notify.notified().await;
				loop {
					match worker.flush_route_batch().await {
						Ok(0) => break,
						Ok(_) => {},
						Err(error) => {
							tracing::warn!(wid = %worker.wid(), %error, "orch route write failed");
							tokio::time::sleep(ROUTE_RETRY_DELAY).await;
						},
					}
				}
			}
		})
	}

	/// Run the worker: spawn the route writer, then loop the liveness task —
	/// list sandboxes off the async runtime, publish one heartbeat, sleep one
	/// cadence; failures log and the loop continues. The returned handle is
	/// the liveness task; aborting it also aborts the route writer.
	pub fn spawn(self: &Arc<Self>, engine: Arc<dyn EngineApi>) -> tokio::task::JoinHandle<()> {
		let worker = Arc::clone(self);
		let route_writer = AbortOnDrop(self.spawn_route_writer());
		tokio::spawn(async move {
			let _route_writer = route_writer;
			loop {
				let list_engine = Arc::clone(&engine);
				match tokio::task::spawn_blocking(move || list_engine.orchestration_inventory()).await {
					Ok(Ok(views)) => {
						if let Err(error) = worker.publish_once(&views).await {
							tracing::warn!(wid = %worker.wid(), %error, "orch heartbeat publish failed");
						}
					},
					Ok(Err(error)) => {
						tracing::warn!(wid = %worker.wid(), %error, "orch sandbox list failed");
					},
					Err(error) => {
						tracing::warn!(wid = %worker.wid(), %error, "orch sandbox list task failed");
					},
				}
				tokio::time::sleep(worker.options.heartbeat).await;
			}
		})
	}
}

#[cfg(target_os = "linux")]
fn host_available_memory_mib() -> io::Result<u64> {
	let mut meminfo = [0_u8; 4096];
	let mut file = std::fs::File::open("/proc/meminfo")?;
	let mut filled = 0;
	while filled < meminfo.len() {
		let read = file.read(&mut meminfo[filled..])?;
		if read == 0 {
			break;
		}
		filled += read;
	}
	let meminfo = std::str::from_utf8(&meminfo[..filled])
		.map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
	parse_linux_available_memory_mib(meminfo).ok_or_else(|| {
		io::Error::new(io::ErrorKind::InvalidData, "/proc/meminfo has no valid MemAvailable")
	})
}

#[cfg(target_os = "linux")]
fn parse_linux_available_memory_mib(meminfo: &str) -> Option<u64> {
	let kib = meminfo.lines().find_map(|line| {
		let value = line.strip_prefix("MemAvailable:")?;
		value.split_whitespace().next()?.parse::<u64>().ok()
	})?;
	Some(kib / 1024)
}

#[cfg(target_os = "macos")]
fn host_available_memory_mib() -> io::Result<u64> {
	let mut stats = MaybeUninit::<libc::vm_statistics64_data_t>::uninit();
	let mut count = libc::HOST_VM_INFO64_COUNT;
	// SAFETY: `stats` points to storage sized for `HOST_VM_INFO64_COUNT`; Mach
	// initializes it on success and `count` is a valid in/out pointer.
	let status = unsafe {
		libc::host_statistics64(
			mach_host_self(),
			libc::HOST_VM_INFO64,
			stats.as_mut_ptr().cast::<libc::integer_t>(),
			&raw mut count,
		)
	};
	if status != libc::KERN_SUCCESS {
		return Err(io::Error::other(format!("host_statistics64 failed ({status})")));
	}
	// SAFETY: a successful `host_statistics64` initialized the structure.
	let stats = unsafe { stats.assume_init() };
	// `inactive` and `speculative` pages are reclaimable without swapping.
	let available_pages = u64::from(stats.free_count)
		.saturating_add(u64::from(stats.inactive_count))
		.saturating_add(u64::from(stats.speculative_count));
	// SAFETY: `sysconf` takes an integer name and returns process-global data.
	let page_size = unsafe { libc::sysconf(libc::_SC_PAGE_SIZE) };
	let page_size =
		u64::try_from(page_size).map_err(|_| io::Error::other("invalid host page size"))?;
	Ok(available_pages.saturating_mul(page_size) / 1_048_576)
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn host_available_memory_mib() -> io::Result<u64> {
	Err(io::Error::new(
		io::ErrorKind::Unsupported,
		"host available-memory probing is unsupported on this platform",
	))
}

/// Sandbox id used on the wire: `name` when present, else `id`.
fn view_sid(view: &Value) -> &str {
	view
		.get("name")
		.and_then(Value::as_str)
		.filter(|name| !name.is_empty())
		.or_else(|| view.get("id").and_then(Value::as_str))
		.unwrap_or_default()
}

/// Engine lifecycle state of a sandbox view.
fn view_state(view: &Value) -> &str {
	view
		.get("status")
		.and_then(Value::as_str)
		.unwrap_or_default()
}

/// Read the stable worker id from `$VMON_HOME/orch-id`, creating it on first
/// use.
pub fn load_or_create_worker_id(home: &Home) -> Result<String> {
	let path = home.root().join("orch-id");
	if let Ok(existing) = std::fs::read_to_string(&path) {
		let id = existing.trim();
		if !id.is_empty() {
			return Ok(id.to_owned());
		}
	}
	let id = crate::mesh::state::new_node_id();
	std::fs::create_dir_all(home.root())?;
	std::fs::write(&path, format!("{id}\n"))?;
	Ok(id)
}

#[cfg(test)]
mod tests {
	use serde_json::json;

	use super::{
		super::{miniredis::MiniRedis, now_ms},
		*,
	};
	use crate::ErrorCode;

	fn worker(redis_url: &str, max_sandboxes: u64) -> Arc<OrchWorker> {
		OrchWorker::new(OrchWorkerOptions {
			redis_url: redis_url.to_owned(),
			wid: "w1".to_owned(),
			url: "http://worker:8000".to_owned(),
			arch: "aarch64".to_owned(),
			backend: "hvf".to_owned(),
			caps: Resources { vcpus: 8, mem_mib: 16_384 },
			heartbeat: Duration::from_millis(10),
			dead_after: Duration::from_millis(250),
			max_sandboxes,
			memory_reserve_mib: 0,
		})
		.expect("worker")
	}

	fn view(sid: &str, status: &str, cpus: u64, memory: u64) -> Value {
		json!({ "id": sid, "name": sid, "status": status, "cpus": cpus, "memory": memory })
	}

	#[test]
	fn build_heartbeat_counts_live_views_and_flips_accepting() {
		let worker = worker("redis://127.0.0.1:1", 0);
		let views = [
			view("sb-a", "running", 2, 1024),
			view("sb-b", "paused", 1, 512),
			view("sb-c", "stopped", 4, 4096),
		];
		let heartbeat = worker.build_heartbeat_with_available(&views, Some(8192));
		assert_eq!(heartbeat.running, 2, "stopped views must not count");
		assert_eq!(heartbeat.used, Resources { vcpus: 3, mem_mib: 8192 });
		assert_eq!(heartbeat.caps, Resources { vcpus: 8, mem_mib: 16_384 });
		assert!(heartbeat.accepting, "unlimited worker with headroom accepts");
		assert!(!heartbeat.draining);
		assert!(heartbeat.ts_ms > 0 && heartbeat.ts_ms <= now_ms());
		let next = worker.build_heartbeat_with_available(&views, Some(8192));
		assert_eq!(next.seq, heartbeat.seq + 1, "seq advances per heartbeat");

		let bounded = self::worker("redis://127.0.0.1:1", 2);
		assert!(
			!bounded
				.build_heartbeat_with_available(&views, Some(8192))
				.accepting,
			"2 live >= max 2"
		);
		assert!(
			bounded
				.build_heartbeat_with_available(&views[1..], Some(8192))
				.accepting,
			"1 live < max 2"
		);
	}

	#[test]
	fn admission_uses_memory_headroom_and_optional_count_ceiling() {
		let memory_bound = worker("redis://127.0.0.1:1", 0);
		let mut guards = Vec::new();
		for _ in 0..32 {
			guards.push(
				memory_bound
					.admit_with_available(32_768)
					.expect("ample headroom must allow well past three sandboxes"),
			);
		}
		assert_eq!(memory_bound.inflight.load(Ordering::SeqCst), 32);
		drop(guards);

		let mut reserved = worker("redis://127.0.0.1:1", 0);
		Arc::get_mut(&mut reserved)
			.expect("sole worker reference")
			.options
			.memory_reserve_mib = 16_384;
		let rejected = reserved
			.admit_with_available(16_384)
			.expect_err("admission must reject at the reserve");
		assert_eq!(rejected.code, ErrorCode::Busy);
		assert_eq!(reserved.inflight.load(Ordering::SeqCst), 0);
		let _headroom = reserved
			.admit_with_available(16_385)
			.expect("one MiB above the reserve remains admissible");

		let bounded = worker("redis://127.0.0.1:1", 2);
		let first = bounded.admit_with_available(u64::MAX).expect("first slot");
		let _second = bounded.admit_with_available(u64::MAX).expect("second slot");
		let rejected = bounded
			.admit_with_available(u64::MAX)
			.expect_err("hard ceiling must reject third sandbox");
		assert_eq!(rejected.code, ErrorCode::Busy);
		drop(first);
		let _third = bounded
			.admit_with_available(u64::MAX)
			.expect("slot freed by dropped guard");
		bounded.draining.store(true, Ordering::SeqCst);
		let drained = bounded
			.admit_with_available(u64::MAX)
			.expect_err("draining must reject");
		assert_eq!(drained.code, ErrorCode::Busy);
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn linux_available_memory_uses_memavailable() {
		let meminfo = "MemTotal:       131072000 kB\nMemFree: 1024 kB\nMemAvailable:   33554432 kB\n";
		assert_eq!(parse_linux_available_memory_mib(meminfo), Some(32_768));
		assert_eq!(parse_linux_available_memory_mib("MemFree: 12 kB\n"), None);
	}

	#[tokio::test]
	async fn publish_and_flush_write_worker_key_and_routes_only_on_change() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let worker = worker(&server.url(), 0);
		let redis = Redis::new(&server.url()).expect("client");

		let views = vec![view("sb-a", "running", 1, 256)];
		worker.publish_once(&views).await.expect("first publish");
		let raw = redis
			.get(&keys::worker("w1"))
			.await
			.expect("get worker key")
			.expect("worker key present");
		let heartbeat: WorkerHeartbeat = serde_json::from_slice(&raw).expect("decode heartbeat");
		assert_eq!(heartbeat.wid, "w1");
		assert_eq!(heartbeat.running, 1);
		assert_eq!(
			redis.get(&keys::sandbox("sb-a")).await.expect("get route"),
			None,
			"liveness publish alone must not write routes"
		);
		assert_eq!(worker.flush_route_batch().await.expect("flush"), 1);
		let raw = redis
			.get(&keys::sandbox("sb-a"))
			.await
			.expect("get route")
			.expect("route written on first sight");
		let route: SandboxRoute = serde_json::from_slice(&raw).expect("decode route");
		assert_eq!(route.wid, "w1");
		assert_eq!(route.state, "running");
		assert_eq!(route.result, None);

		// Unchanged snapshot: nothing is queued, the deleted route must not
		// reappear.
		redis.del(&keys::sandbox("sb-a")).await.expect("del route");
		worker.publish_once(&views).await.expect("second publish");
		assert_eq!(worker.flush_route_batch().await.expect("flush"), 0);
		assert_eq!(
			redis.get(&keys::sandbox("sb-a")).await.expect("get route"),
			None,
			"unchanged state must not rewrite the route"
		);

		// State change: route rewritten with the exit code.
		let mut stopped = view("sb-a", "stopped", 1, 256);
		stopped["returncode"] = json!(0);
		worker
			.publish_once(&[stopped])
			.await
			.expect("third publish");
		assert_eq!(worker.flush_route_batch().await.expect("flush"), 1);
		let raw = redis
			.get(&keys::sandbox("sb-a"))
			.await
			.expect("get route")
			.expect("changed state rewrites the route");
		let route: SandboxRoute = serde_json::from_slice(&raw).expect("decode route");
		assert_eq!(route.state, "stopped");
		assert_eq!(route.result, Some(0));

		// Removal: a sandbox that vanished from the engine terminates its
		// route, keeping the last known exit code.
		worker.publish_once(&[]).await.expect("fourth publish");
		assert_eq!(worker.flush_route_batch().await.expect("flush"), 1);
		let raw = redis
			.get(&keys::sandbox("sb-a"))
			.await
			.expect("get route")
			.expect("removed sandbox writes a terminal route");
		let route: SandboxRoute = serde_json::from_slice(&raw).expect("decode route");
		assert_eq!(route.state, "terminated");
		assert_eq!(route.result, Some(0));
		// The tombstone is written exactly once.
		redis.del(&keys::sandbox("sb-a")).await.expect("del route");
		worker.publish_once(&[]).await.expect("fifth publish");
		assert_eq!(worker.flush_route_batch().await.expect("flush"), 0);
		assert_eq!(redis.get(&keys::sandbox("sb-a")).await.expect("get route"), None);
	}

	#[tokio::test]
	async fn route_queue_coalesces_latest_state_per_sandbox() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let worker = worker(&server.url(), 0);
		let redis = Redis::new(&server.url()).expect("client");

		worker
			.publish_once(&[view("sb-a", "running", 1, 256)])
			.await
			.expect("publish running");
		let mut stopped = view("sb-a", "stopped", 1, 256);
		stopped["returncode"] = json!(7);
		worker
			.publish_once(&[stopped])
			.await
			.expect("publish stopped");
		assert_eq!(worker.route_queue.len(), 1, "changes must coalesce per sid");

		assert_eq!(worker.flush_route_batch().await.expect("flush"), 1);
		let raw = redis
			.get(&keys::sandbox("sb-a"))
			.await
			.expect("get route")
			.expect("route present");
		let route: SandboxRoute = serde_json::from_slice(&raw).expect("decode route");
		assert_eq!(route.state, "stopped", "only the latest state is written");
		assert_eq!(route.result, Some(7));
		assert_eq!(worker.flush_route_batch().await.expect("empty flush"), 0);
	}

	/// The liveness invariant: with the route writer wedged on a dead-slow
	/// connection (accepted, never answered), liveness publishes keep landing
	/// promptly on their own connection. If the two paths ever shared a
	/// handle again, each publish would stall behind the pipeline's command
	/// timeout and this test would blow its bound.
	#[tokio::test]
	async fn stalled_route_writer_never_delays_liveness() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let redis = Redis::new(&server.url()).expect("client");

		// Black hole: accepts connections and keeps them open, never replies.
		let hole = tokio::net::TcpListener::bind("127.0.0.1:0")
			.await
			.expect("bind hole");
		let hole_addr = hole.local_addr().expect("hole addr");
		let hole_task = tokio::spawn(async move {
			#[allow(
				clippy::collection_is_never_read,
				reason = "retains TCP streams to keep connections open in black-hole server"
			)]
			let mut sockets = Vec::new();
			loop {
				let Ok((socket, _peer)) = hole.accept().await else {
					break;
				};
				sockets.push(socket);
			}
		});

		let mut worker = worker(&server.url(), 0);
		Arc::get_mut(&mut worker)
			.expect("sole reference")
			.route_redis = Redis::new(&format!("redis://{hole_addr}")).expect("hole client");
		let writer = worker.spawn_route_writer();

		// First publish enqueues one route change; the writer picks it up and
		// wedges against the black hole.
		let views = vec![view("sb-a", "running", 1, 256)];
		worker.publish_once(&views).await.expect("first publish");
		tokio::task::yield_now().await;

		let started = tokio::time::Instant::now();
		for round in 0..5 {
			worker
				.publish_once(&views)
				.await
				.unwrap_or_else(|error| panic!("liveness publish {round} failed: {error}"));
		}
		assert!(
			started.elapsed() < Duration::from_secs(2),
			"liveness publishes must not wait on the stalled route pipeline"
		);
		let raw = redis
			.get(&keys::worker("w1"))
			.await
			.expect("get worker key")
			.expect("worker key present");
		let heartbeat: WorkerHeartbeat = serde_json::from_slice(&raw).expect("decode heartbeat");
		assert_eq!(heartbeat.seq, 6, "every liveness publish landed");
		assert_eq!(
			redis.get(&keys::sandbox("sb-a")).await.expect("get route"),
			None,
			"the wedged writer must not have reached the real server"
		);

		writer.abort();
		hole_task.abort();
	}

	#[tokio::test]
	async fn drain_key_stops_admission_via_publish() {
		let server = MiniRedis::spawn().await.expect("spawn");
		let worker = worker(&server.url(), 0);
		let redis = Redis::new(&server.url()).expect("client");
		redis
			.set_px(&keys::drain("w1"), b"1", 60_000)
			.await
			.expect("set drain");
		worker.publish_once(&[]).await.expect("publish");
		let raw = redis
			.get(&keys::worker("w1"))
			.await
			.expect("get worker key")
			.expect("worker key present");
		let heartbeat: WorkerHeartbeat = serde_json::from_slice(&raw).expect("decode heartbeat");
		assert!(heartbeat.draining);
		assert!(!heartbeat.accepting);
		let rejected = worker.admit().expect_err("draining worker must reject");
		assert_eq!(rejected.code, ErrorCode::Busy);
	}
}
