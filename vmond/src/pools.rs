//! Warm pools: ready clones + background refiller. Port of python/vmon/pool.py.

use std::{
	collections::{BTreeMap, VecDeque},
	fs,
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use parking_lot::Mutex;

use crate::{
	Result,
	engine::{
		agent::AgentConn,
		spawn::{LaunchSpec, SandboxRuntime, SandboxVm, VmonRuntime},
	},
};

const REFILL_POLL: Duration = Duration::from_millis(100);
const DEFAULT_PING_TIMEOUT: Duration = Duration::from_secs(30);
const MAX_REFILL_BATCH: usize = 32;
/// Pool members are parked paused by default: a claim is then just
/// rename + resume, with zero guest activity (and zero timeout drift) while
/// the clone waits in the deque. Members whose runtime cannot pause stay
/// running and are claimed without a resume.
const POOL_MEMBERS_PAUSED: bool = true;
static NEXT_POOL_ID: AtomicU64 = AtomicU64::new(1);

/// Format the portable template key used by placement and pool APIs.
pub fn template_key(
	reference: &str,
	disk_mb: u64,
	memory: u64,
	cpus: u64,
	fs_slots: u64,
	host_slot: bool,
	nic_slot: bool,
	tap_slot: bool,
) -> String {
	format!(
		"{reference}|d{disk_mb}|m{memory}|c{cpus}|s{fs_slots}|h{}|n{}|t{}",
		u8::from(host_slot),
		u8::from(nic_slot),
		u8::from(tap_slot),
	)
}

/// One warm-pool statistics snapshot.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct PoolStats {
	pub ready:  usize,
	pub hits:   u64,
	pub misses: u64,
	pub size:   usize,
}

/// One parked clone plus whether its vCPUs are currently paused.
struct PooledVm {
	vm:     SandboxVm,
	paused: bool,
}

/// A background-filled deque of restored/forked, agent-pinged VMs.
pub struct WarmPool {
	template:     PathBuf,
	has_rootfs:   bool,
	size:         AtomicUsize,
	fork:         bool,
	agent:        bool,
	paused:       bool,
	ping_timeout: Duration,
	runtime:      Arc<dyn SandboxRuntime>,
	ready:        Mutex<VecDeque<PooledVm>>,
	stop:         AtomicBool,
	hits:         AtomicU64,
	misses:       AtomicU64,
	refiller:     Mutex<Option<JoinHandle<()>>>,
}

impl WarmPool {
	/// Start a refiller for `template` with `size` parked VMs.
	pub fn new(template: impl Into<PathBuf>, size: usize) -> Result<Arc<Self>> {
		Self::with_runtime(template, size, Arc::new(VmonRuntime))
	}

	/// Start a refiller using the selected sandbox runtime.
	pub fn with_runtime(
		template: impl Into<PathBuf>,
		size: usize,
		runtime: Arc<dyn SandboxRuntime>,
	) -> Result<Arc<Self>> {
		Self::with_runtime_options(
			template,
			size,
			true,
			true,
			POOL_MEMBERS_PAUSED,
			DEFAULT_PING_TIMEOUT,
			runtime,
		)
	}

	/// Start a configurable refiller, mostly used by tests and restore-only
	/// pools.
	pub fn with_options(
		template: impl Into<PathBuf>,
		size: usize,
		fork: bool,
		agent: bool,
		ping_timeout: Duration,
	) -> Result<Arc<Self>> {
		Self::with_runtime_options(
			template,
			size,
			fork,
			agent,
			POOL_MEMBERS_PAUSED,
			ping_timeout,
			Arc::new(VmonRuntime),
		)
	}

	pub(crate) fn with_runtime_options(
		template: impl Into<PathBuf>,
		size: usize,
		fork: bool,
		agent: bool,
		paused: bool,
		ping_timeout: Duration,
		runtime: Arc<dyn SandboxRuntime>,
	) -> Result<Arc<Self>> {
		let template = template.into();
		let has_rootfs = template.join("rootfs.img").is_file();
		let pool = Arc::new(Self {
			template,
			has_rootfs,
			size: AtomicUsize::new(size),
			fork,
			agent,
			paused,
			ping_timeout,
			runtime,
			ready: Mutex::new(VecDeque::new()),
			stop: AtomicBool::new(false),
			hits: AtomicU64::new(0),
			misses: AtomicU64::new(0),
			refiller: Mutex::new(None),
		});
		let thread_pool = Arc::clone(&pool);
		let handle = thread::Builder::new()
			.name("vmon-warmpool-refiller".to_owned())
			.spawn(move || thread_pool.refill_loop())?;
		*pool.refiller.lock() = Some(handle);
		Ok(pool)
	}

	/// Template snapshot directory backing this pool.
	pub fn template(&self) -> &Path {
		&self.template
	}

	/// Resize the target ready depth. The refiller observes this immediately.
	pub fn resize(&self, size: usize) {
		self.size.store(size, Ordering::Relaxed);
		let mut ready = self.ready.lock();
		while ready.len() > size {
			if let Some(member) = ready.pop_back() {
				self.teardown(member.vm);
			}
		}
	}

	/// Pop a ready clone in O(1): rename it to the requested sandbox name and
	/// bring its vCPUs into the wanted state (paused members resume unless the
	/// caller stages a paused candidate). A member that cannot reach the
	/// wanted state is torn down and the next one is tried; the claim only
	/// misses when the deque is empty.
	pub fn claim(&self, name: Option<&str>, want_paused: bool) -> Result<Option<SandboxVm>> {
		loop {
			let Some(PooledVm { vm, paused }) = self.ready.lock().pop_front() else {
				self.misses.fetch_add(1, Ordering::Relaxed);
				return Ok(None);
			};
			let vm = match name {
				Some(name) if vm.name() != name => match rename_vm(&vm, name) {
					Ok(renamed) => renamed,
					Err(error) => {
						self.teardown(vm);
						return Err(error);
					},
				},
				_ => vm,
			};
			let transition = match (paused, want_paused) {
				(true, false) => self.runtime.resume(&vm),
				(false, true) => self.runtime.pause(&vm),
				_ => Ok(()),
			};
			if transition.is_err() {
				self.teardown(vm);
				continue;
			}
			self.hits.fetch_add(1, Ordering::Relaxed);
			return Ok(Some(vm));
		}
	}

	/// Snapshot hit/miss counters and ready depth.
	pub fn stats(&self) -> PoolStats {
		PoolStats {
			ready:  self.ready.lock().len(),
			hits:   self.hits.load(Ordering::Relaxed),
			misses: self.misses.load(Ordering::Relaxed),
			size:   self.size.load(Ordering::Relaxed),
		}
	}

	/// Stop the refiller and tear down still-parked clones. Claimed VMs are
	/// untouched.
	pub fn shutdown(&self) {
		self.stop.store(true, Ordering::Relaxed);
		let handle = self.refiller.lock().take();
		if let Some(handle) = handle {
			let _ = handle.join();
		}
		let parked = self.ready.lock().drain(..).collect::<Vec<_>>();
		for member in parked {
			self.teardown(member.vm);
		}
	}

	fn refill_loop(self: Arc<Self>) {
		while !self.stop.load(Ordering::Relaxed) {
			let ready = self.ready.lock().len();
			let target = self.size.load(Ordering::Relaxed);
			if ready < target {
				self.refill_batch(target - ready);
			}
			thread::sleep(REFILL_POLL);
		}
	}

	/// Refill independent pool slots concurrently. Only this background thread
	/// produces ready VMs, so launches do not race; capacity is checked again
	/// before publishing results to make a concurrent resize safe.
	fn refill_batch(&self, shortfall: usize) {
		let count = shortfall.min(MAX_REFILL_BATCH);
		if count == 0 {
			return;
		}
		let spawned = thread::scope(|scope| {
			let workers = (0..count)
				.map(|_| scope.spawn(|| self.spawn_one()))
				.collect::<Vec<_>>();
			workers
				.into_iter()
				.filter_map(|worker| worker.join().ok().flatten())
				.collect::<Vec<_>>()
		});

		let mut overflow = Vec::new();
		{
			let mut ready = self.ready.lock();
			let target = self.size.load(Ordering::Relaxed);
			for member in spawned {
				if self.stop.load(Ordering::Relaxed) || ready.len() >= target {
					overflow.push(member);
				} else {
					ready.push_back(member);
				}
			}
		}
		for member in overflow {
			self.teardown(member.vm);
		}
	}

	fn spawn_one(&self) -> Option<PooledVm> {
		let vm_name = pooled_name(&self.template);
		let vm = self.runtime.sandbox(&vm_name);
		let mut spec = if self.fork {
			LaunchSpec::fork_from(vm.api_sock(), &self.template)
		} else {
			LaunchSpec::restore(vm.api_sock(), &self.template)
		};
		// Pool VMs must not share the template's writable image: overlay it so
		// each claimed sandbox owns a checkpointable per-VM disk. The VMM
		// materializes the overlay copy-on-write (reflink where the filesystem
		// supports it); the template image itself stays immutable.
		if self.has_rootfs {
			let base_disk = self.template.join("rootfs.img");
			spec = spec.with_disk_overlay(base_disk, vm.dir().join("rootfs.img"));
		}
		if self.agent {
			spec = spec.with_agent_sock(vm.dir().join("agent.sock"));
		}
		if let Err(_err) = self.runtime.launch(&vm, &spec) {
			let _ = self.runtime.remove(&vm);
			return None;
		}
		if self.agent {
			let readiness =
				AgentConn::connect(&vm.agent_sock().ok()?, self.ping_timeout).and_then(|agent| {
					let result = agent.ping(self.ping_timeout).map(|_| ());
					agent.close();
					result
				});
			if let Err(_err) = readiness {
				self.teardown(vm);
				return None;
			}
		}
		// Park the member paused so a waiting clone burns no guest CPU and a
		// claim is exactly rename + resume. A runtime that cannot pause keeps
		// the member running; claims then skip the resume.
		let paused = self.paused && self.runtime.pause(&vm).is_ok();
		Some(PooledVm { vm, paused })
	}

	fn teardown(&self, vm: SandboxVm) {
		let _ = self.runtime.stop(&vm, true);
		let _ = self.runtime.remove(&vm);
	}
}

impl Drop for WarmPool {
	fn drop(&mut self) {
		self.stop.store(true, Ordering::Relaxed);
	}
}

/// Server-owned registry of warm pools keyed by portable template reference.
#[derive(Default)]
pub struct PoolRegistry {
	pools: Mutex<BTreeMap<String, Arc<WarmPool>>>,
}

impl PoolRegistry {
	pub fn new() -> Self {
		Self::default()
	}

	pub fn get(&self, reference: &str) -> Option<Arc<WarmPool>> {
		self.pools.lock().get(reference).cloned()
	}

	pub fn set(&self, reference: impl Into<String>, pool: Arc<WarmPool>) -> Option<Arc<WarmPool>> {
		self.pools.lock().insert(reference.into(), pool)
	}

	pub fn delete(&self, reference: &str) -> Option<Arc<WarmPool>> {
		self.pools.lock().remove(reference)
	}

	pub fn list(&self) -> BTreeMap<String, PoolStats> {
		self
			.pools
			.lock()
			.iter()
			.map(|(reference, pool)| (reference.clone(), pool.stats()))
			.collect()
	}

	pub fn shutdown(&self) {
		let pools = self.pools.lock().values().cloned().collect::<Vec<_>>();
		for pool in pools {
			pool.shutdown();
		}
	}

	pub fn total_hits_misses(&self) -> (u64, u64) {
		self
			.pools
			.lock()
			.values()
			.map(|pool| {
				let stats = pool.stats();
				(stats.hits, stats.misses)
			})
			.fold((0, 0), |(hits, misses), (pool_hits, pool_misses)| {
				(hits + pool_hits, misses + pool_misses)
			})
	}
}

fn rename_vm(vm: &SandboxVm, name: &str) -> Result<SandboxVm> {
	let old_meta = vm.meta()?;
	let old_dir = vm.dir().to_path_buf();
	let parent = old_dir
		.parent()
		.ok_or_else(|| crate::EngineError::invalid("VM directory must have a parent"))?;
	let new_dir = parent.join(name);
	if new_dir.exists() {
		return Err(crate::EngineError::busy(format!("sandbox '{name}' already exists")));
	}
	let renamed = SandboxVm::from_dir(name.to_owned(), new_dir.clone());
	let mut meta = serde_json::Map::new();
	meta.insert("sock".to_owned(), serde_json::json!(renamed.api_sock().to_string_lossy()));
	if let Some(value) = old_meta.get("agent_sock") {
		meta.insert(
			"agent_sock".to_owned(),
			renamed_path_meta(value, &renamed.dir().join("agent.sock"))?,
		);
	}
	if let Some(value) = old_meta.get("rootfs") {
		meta
			.insert("rootfs".to_owned(), renamed_path_meta(value, &renamed.dir().join("rootfs.img"))?);
	}
	// Persist the future paths before the atomic directory rename. If either
	// step fails, claim still owns the original handle and can tear it down.
	vm.save_meta(meta)?;
	fs::rename(old_dir, new_dir)?;
	Ok(renamed)
}

fn renamed_path_meta(value: &serde_json::Value, replacement: &Path) -> Result<serde_json::Value> {
	match value {
		serde_json::Value::Null | serde_json::Value::Bool(false) => Ok(serde_json::Value::Null),
		serde_json::Value::String(raw) if raw.is_empty() => Ok(serde_json::Value::Null),
		serde_json::Value::String(_) => Ok(serde_json::json!(replacement.to_string_lossy())),
		_ => Err(crate::EngineError::invalid("metadata path must be str or null")),
	}
}

fn pooled_name(template: &Path) -> String {
	let stem = template
		.file_name()
		.and_then(|name| name.to_str())
		.unwrap_or("template")
		.chars()
		.map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
		.take(32)
		.collect::<String>();
	let seq = NEXT_POOL_ID.fetch_add(1, Ordering::Relaxed);
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_nanos() as u64);
	format!("_pool-{stem}-{seq:08x}{:08x}", nanos & 0xffff_ffff)
}

#[cfg(test)]
mod tests {
	use std::time::Instant;

	use super::*;
	use crate::engine::spawn::LaunchMode;

	#[test]
	fn template_key_uses_locked_format() {
		assert_eq!(
			template_key("debian:stable", 1024, 512, 2, 3, true, false, true),
			"debian:stable|d1024|m512|c2|s3|h1|n0|t1",
		);
	}

	#[test]
	fn renamed_path_meta_preserves_nullish_values() -> Result<()> {
		let replacement = Path::new("/tmp/new-rootfs.img");
		assert!(renamed_path_meta(&serde_json::Value::Null, replacement)?.is_null());
		assert!(renamed_path_meta(&serde_json::Value::Bool(false), replacement)?.is_null());
		assert!(renamed_path_meta(&serde_json::Value::String(String::new()), replacement)?.is_null());
		assert_eq!(
			renamed_path_meta(&serde_json::Value::String("/old".to_owned()), replacement)?.as_str(),
			Some("/tmp/new-rootfs.img"),
		);
		Ok(())
	}

	#[test]
	fn rename_rewrites_present_paths_and_preserves_nulls() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let old_dir = tmp.path().join("vms").join("old");
		fs::create_dir_all(&old_dir)?;
		let vm = SandboxVm::from_dir("old", &old_dir);
		let mut meta = serde_json::Map::new();
		meta.insert(
			"agent_sock".to_owned(),
			serde_json::json!(old_dir.join("agent.sock").to_string_lossy()),
		);
		meta.insert(
			"rootfs".to_owned(),
			serde_json::json!(old_dir.join("rootfs.img").to_string_lossy()),
		);
		meta.insert("remote_page_url".to_owned(), serde_json::Value::Null);
		vm.save_meta(meta)?;

		let renamed = rename_vm(&vm, "new")?;
		let meta = renamed.meta()?;
		assert_eq!(
			meta.get("sock").and_then(serde_json::Value::as_str),
			Some(
				tmp.path()
					.join("vms/new/api.sock")
					.to_string_lossy()
					.as_ref()
			),
		);
		assert_eq!(
			meta.get("agent_sock").and_then(serde_json::Value::as_str),
			Some(
				tmp.path()
					.join("vms/new/agent.sock")
					.to_string_lossy()
					.as_ref()
			),
		);
		assert_eq!(
			meta.get("rootfs").and_then(serde_json::Value::as_str),
			Some(
				tmp.path()
					.join("vms/new/rootfs.img")
					.to_string_lossy()
					.as_ref()
			),
		);
		assert!(
			meta
				.get("remote_page_url")
				.is_some_and(serde_json::Value::is_null)
		);
		Ok(())
	}

	#[test]
	fn registry_stats_are_keyed_by_reference() {
		let registry = PoolRegistry::new();
		let pool = WarmPool::with_options("/no/template", 0, false, false, Duration::ZERO)
			.expect("pool starts");
		registry.set("ref", Arc::clone(&pool));
		let stats = registry.list();
		assert_eq!(stats["ref"], PoolStats { ready: 0, hits: 0, misses: 0, size: 0 });
		registry.shutdown();
	}
	#[derive(Debug)]
	struct RecordingRuntime {
		root:   PathBuf,
		events: Mutex<Vec<&'static str>>,
	}

	impl SandboxRuntime for RecordingRuntime {
		fn name(&self) -> &'static str {
			"recording"
		}

		fn sandbox(&self, name: &str) -> SandboxVm {
			SandboxVm::from_dir(name, self.root.join(name))
		}

		fn launch(&self, vm: &SandboxVm, _spec: &LaunchSpec) -> Result<()> {
			fs::create_dir_all(vm.dir())?;
			self.events.lock().push("launch");
			Ok(())
		}

		fn stop(&self, _vm: &SandboxVm, _wait: bool) -> Result<()> {
			self.events.lock().push("stop");
			Ok(())
		}

		fn remove(&self, vm: &SandboxVm) -> Result<()> {
			self.events.lock().push("remove");
			fs::remove_dir_all(vm.dir())?;
			Ok(())
		}

		fn is_running(&self, _vm: &SandboxVm) -> Result<bool> {
			Ok(true)
		}
	}

	#[test]
	fn warm_pool_delegates_lifecycle_to_selected_runtime() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let runtime = Arc::new(RecordingRuntime {
			root:   tmp.path().join("vms"),
			events: Mutex::new(Vec::new()),
		});
		let backend: Arc<dyn SandboxRuntime> = runtime.clone();
		let pool = WarmPool::with_runtime_options(
			tmp.path().join("template"),
			1,
			false,
			false,
			false,
			Duration::ZERO,
			backend,
		)?;
		let deadline = Instant::now() + Duration::from_secs(1);
		while pool.stats().ready == 0 && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(1));
		}
		assert_eq!(pool.stats().ready, 1);

		pool.shutdown();
		assert_eq!(*runtime.events.lock(), ["launch", "stop", "remove"]);
		Ok(())
	}
	#[derive(Debug)]
	struct DelayedRuntime {
		root:             PathBuf,
		delay:            Duration,
		concurrency_peak: Mutex<usize>,
		concurrency_now:  Mutex<usize>,
	}

	impl SandboxRuntime for DelayedRuntime {
		fn name(&self) -> &'static str {
			"delayed"
		}

		fn sandbox(&self, name: &str) -> SandboxVm {
			SandboxVm::from_dir(name, self.root.join(name))
		}

		fn launch(&self, vm: &SandboxVm, _spec: &LaunchSpec) -> Result<()> {
			fs::create_dir_all(vm.dir())?;
			{
				let mut now = self.concurrency_now.lock();
				*now += 1;
				let mut peak = self.concurrency_peak.lock();
				*peak = (*peak).max(*now);
			}
			thread::sleep(self.delay);
			{
				let mut now = self.concurrency_now.lock();
				*now -= 1;
			}
			Ok(())
		}

		fn stop(&self, _vm: &SandboxVm, _wait: bool) -> Result<()> {
			Ok(())
		}

		fn remove(&self, vm: &SandboxVm) -> Result<()> {
			let _ = fs::remove_dir_all(vm.dir());
			Ok(())
		}

		fn is_running(&self, _vm: &SandboxVm) -> Result<bool> {
			Ok(true)
		}
	}

	#[test]
	fn warm_pool_refills_concurrently_and_respects_target_bounds() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let delay = Duration::from_millis(50);
		let runtime = Arc::new(DelayedRuntime {
			root: tmp.path().join("vms"),
			delay,
			concurrency_peak: Mutex::new(0),
			concurrency_now: Mutex::new(0),
		});

		// Execute 32 launches sequentially as a baseline for the maximum batch.
		let t_seq_start = Instant::now();
		for i in 0..32 {
			let vm = runtime.sandbox(&format!("serial-{i}"));
			let spec = LaunchSpec::restore(vm.api_sock(), tmp.path().join("template"));
			runtime.launch(&vm, &spec)?;
		}
		let elapsed_seq = t_seq_start.elapsed();

		// Execute the same 32 launches through the bounded concurrent refiller.
		let backend: Arc<dyn SandboxRuntime> = runtime.clone();
		let t_concurrent_start = Instant::now();
		let pool = WarmPool::with_runtime_options(
			tmp.path().join("template"),
			32,
			false,
			false,
			false,
			Duration::ZERO,
			backend,
		)?;

		let deadline = Instant::now() + Duration::from_secs(4);
		while pool.stats().ready < 32 && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(1));
		}
		let elapsed_concurrent = t_concurrent_start.elapsed();

		assert_eq!(pool.stats().ready, 32, "Concurrent pool failed to reach full size");
		pool.shutdown();

		let peak_concurrency = *runtime.concurrency_peak.lock();
		println!(
			"{}",
			serde_json::json!({
				"kind": "warm_pool_refill",
				"clones": 32,
				"baseline_serial_ms": elapsed_seq.as_millis(),
				"optimized_parallel_ms": elapsed_concurrent.as_millis(),
				"peak_parallel_spawns": peak_concurrency,
			})
		);

		// Assert parallel execution and bounds checks
		assert!(
			peak_concurrency > 1,
			"WarmPool refill did not run launches in parallel: peak={peak_concurrency}"
		);
		assert!(
			elapsed_concurrent < elapsed_seq,
			"Parallel pool refill took longer than sequential baseline"
		);
		Ok(())
	}

	/// Runtime with a working pause/resume control plane. Launch (the template
	/// build/boot path) panics when invoked on the claiming thread: a claim
	/// must be exactly rename + resume, never a rebuild.
	#[derive(Debug)]
	struct PausingRuntime {
		root:        PathBuf,
		claimer:     thread::ThreadId,
		events:      Mutex<Vec<String>>,
		specs:       Mutex<Vec<LaunchSpec>>,
		fail_resume: AtomicBool,
	}

	impl PausingRuntime {
		fn new(root: PathBuf) -> Arc<Self> {
			Arc::new(Self {
				root,
				claimer: thread::current().id(),
				events: Mutex::new(Vec::new()),
				specs: Mutex::new(Vec::new()),
				fail_resume: AtomicBool::new(false),
			})
		}

		fn events(&self) -> Vec<String> {
			self.events.lock().clone()
		}
	}

	impl SandboxRuntime for PausingRuntime {
		fn name(&self) -> &'static str {
			"pausing"
		}

		fn sandbox(&self, name: &str) -> SandboxVm {
			SandboxVm::from_dir(name, self.root.join(name))
		}

		fn launch(&self, vm: &SandboxVm, spec: &LaunchSpec) -> Result<()> {
			assert_ne!(
				thread::current().id(),
				self.claimer,
				"template build/launch path invoked during a warm-pool claim"
			);
			fs::create_dir_all(vm.dir())?;
			self.events.lock().push(format!("launch:{}", vm.name()));
			self.specs.lock().push(spec.clone());
			Ok(())
		}

		fn stop(&self, vm: &SandboxVm, _wait: bool) -> Result<()> {
			self.events.lock().push(format!("stop:{}", vm.name()));
			Ok(())
		}

		fn remove(&self, vm: &SandboxVm) -> Result<()> {
			self.events.lock().push(format!("remove:{}", vm.name()));
			let _ = fs::remove_dir_all(vm.dir());
			Ok(())
		}

		fn is_running(&self, _vm: &SandboxVm) -> Result<bool> {
			Ok(true)
		}

		fn pause(&self, vm: &SandboxVm) -> Result<()> {
			self.events.lock().push(format!("pause:{}", vm.name()));
			Ok(())
		}

		fn resume(&self, vm: &SandboxVm) -> Result<()> {
			if self.fail_resume.load(Ordering::Relaxed) {
				return Err(crate::EngineError::engine("resume rejected by test"));
			}
			self.events.lock().push(format!("resume:{}", vm.name()));
			Ok(())
		}
	}

	fn paused_pool_with_member(
		runtime: &Arc<PausingRuntime>,
		template: &Path,
	) -> Result<Arc<WarmPool>> {
		let backend: Arc<dyn SandboxRuntime> = Arc::clone(runtime) as Arc<dyn SandboxRuntime>;
		let pool =
			WarmPool::with_runtime_options(template, 1, true, false, true, Duration::ZERO, backend)?;
		let deadline = Instant::now() + Duration::from_secs(2);
		while pool.stats().ready == 0 && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(1));
		}
		assert_eq!(pool.stats().ready, 1, "refiller failed to park a member");
		Ok(pool)
	}

	#[test]
	fn claim_is_rename_plus_resume_without_template_rebuild() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let template = tmp.path().join("template");
		fs::create_dir_all(&template)?;
		fs::write(template.join("rootfs.img"), b"base")?;
		let runtime = PausingRuntime::new(tmp.path().join("vms"));
		let pool = paused_pool_with_member(&runtime, &template)?;

		// PausingRuntime::launch panics on this thread: the claim below proves
		// no OCI/template/build work happens inside the claim window.
		let vm = pool
			.claim(Some("wanted"), false)?
			.expect("parked member claims");
		assert_eq!(vm.name(), "wanted");
		assert!(vm.dir().ends_with("vms/wanted"), "claim renames the VM directory");
		assert!(vm.dir().is_dir());
		let events = runtime.events();
		assert!(
			events.iter().any(|event| event.starts_with("pause:_pool-")),
			"refiller parks members paused: {events:?}"
		);
		assert_eq!(
			events.last().map(String::as_str),
			Some("resume:wanted"),
			"claim resumes the renamed clone: {events:?}"
		);
		assert_eq!(pool.stats().hits, 1);
		pool.shutdown();
		Ok(())
	}

	#[test]
	fn paused_claim_keeps_member_paused_and_running_member_gets_paused() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let template = tmp.path().join("template");
		fs::create_dir_all(&template)?;
		let runtime = PausingRuntime::new(tmp.path().join("vms"));
		let pool = paused_pool_with_member(&runtime, &template)?;

		// A staged (start-paused) claim of a paused member transitions nothing.
		let vm = pool
			.claim(Some("staged"), true)?
			.expect("parked member claims");
		assert_eq!(vm.name(), "staged");
		let events = runtime.events();
		assert!(
			!events.iter().any(|event| event == "resume:staged"),
			"staged claim must not resume: {events:?}"
		);
		pool.shutdown();

		// A running member (unpaused pool) claimed for staging gets paused.
		let backend: Arc<dyn SandboxRuntime> = Arc::clone(&runtime) as Arc<dyn SandboxRuntime>;
		let pool =
			WarmPool::with_runtime_options(&template, 1, true, false, false, Duration::ZERO, backend)?;
		let deadline = Instant::now() + Duration::from_secs(2);
		while pool.stats().ready == 0 && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(1));
		}
		let vm = pool
			.claim(Some("staged-2"), true)?
			.expect("running member claims");
		assert_eq!(vm.name(), "staged-2");
		assert!(
			runtime
				.events()
				.iter()
				.any(|event| event == "pause:staged-2"),
			"staging a running member pauses it: {:?}",
			runtime.events()
		);
		pool.shutdown();
		Ok(())
	}

	#[test]
	fn claim_miss_and_unresumable_member_fall_back_to_boot_path() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let template = tmp.path().join("template");
		fs::create_dir_all(&template)?;
		let runtime = PausingRuntime::new(tmp.path().join("vms"));

		// Empty pool: a claim is a miss, so facade::claim_or_launch_vm falls
		// through to the full restore/boot path.
		let backend: Arc<dyn SandboxRuntime> = Arc::clone(&runtime) as Arc<dyn SandboxRuntime>;
		let empty =
			WarmPool::with_runtime_options(&template, 0, true, false, true, Duration::ZERO, backend)?;
		assert!(empty.claim(Some("missed"), false)?.is_none());
		assert_eq!(empty.stats(), PoolStats { ready: 0, hits: 0, misses: 1, size: 0 });
		empty.shutdown();

		// A member whose vCPUs cannot resume is torn down instead of being
		// handed out; the exhausted deque then reports a miss.
		let pool = paused_pool_with_member(&runtime, &template)?;
		runtime.fail_resume.store(true, Ordering::Relaxed);
		assert!(pool.claim(Some("dead"), false)?.is_none());
		let events = runtime.events();
		assert!(
			events.iter().any(|event| event == "stop:dead")
				&& events.iter().any(|event| event == "remove:dead"),
			"unresumable member is torn down: {events:?}"
		);
		assert_eq!(pool.stats().hits, 0);
		pool.shutdown();
		Ok(())
	}

	#[test]
	fn clone_disks_use_cow_overlay_of_template_image() -> Result<()> {
		let tmp = tempfile::tempdir()?;
		let template = tmp.path().join("template");
		fs::create_dir_all(&template)?;
		fs::write(template.join("rootfs.img"), b"base")?;
		let runtime = PausingRuntime::new(tmp.path().join("vms"));
		let pool = paused_pool_with_member(&runtime, &template)?;
		pool.shutdown();

		let specs = runtime.specs.lock();
		let spec = specs.first().expect("refiller captured a launch spec");
		assert_eq!(
			spec.mode,
			LaunchMode::Fork { snapshot: template.clone() },
			"pool members fork from the template snapshot (CoW memory), not boot"
		);
		assert_eq!(
			spec.disk_overlay_of.as_deref(),
			Some(template.join("rootfs.img").as_path()),
			"clone disks overlay the immutable template image"
		);
		let rootfs = spec.rootfs.clone().expect("overlay names a per-VM rootfs");
		assert!(
			rootfs.starts_with(tmp.path().join("vms")) && rootfs.ends_with("rootfs.img"),
			"overlay writes land in the clone's own directory: {}",
			rootfs.display()
		);
		assert_ne!(rootfs, template.join("rootfs.img"));
		Ok(())
	}
}
