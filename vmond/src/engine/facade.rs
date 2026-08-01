//! The real [`Engine`]: method-for-method port of `python/vmon/core.py`.

use std::{
	collections::{BTreeMap, HashMap, HashSet},
	fmt::Write as _,
	fs::{self, OpenOptions},
	io::{self, Read, Seek, SeekFrom, Write as _},
	net::IpAddr,
	ops::Deref,
	os::unix::fs::{OpenOptionsExt, PermissionsExt},
	path::{Path, PathBuf},
	process::Command,
	sync::{
		Arc, Weak,
		atomic::{AtomicBool, AtomicU64, Ordering},
	},
	thread,
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use flume::{Receiver, Sender};
use parking_lot::{Condvar, Mutex};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use tokio::{
	runtime::Runtime,
	sync::{Notify, broadcast},
	task::JoinHandle,
};
use vmm::snapshot::is_safe_snapshot_name;

use crate::{
	config::{ClusterMode, ServeConfig, WarmImage},
	engine::{
		EngineApi, ExecCapture, ExecExit, ExecRequest, ExecStream, OwnershipHandoff, RecoveryPoint,
		ShellSession,
		agent::{AgentConn, ExecHandle, GuestActivity, PtyAgentHandle},
		control::ControlClient,
		diskdelta,
		pty::{PtyCache, PtyControl, PtyStream},
		s3proxy::S3Proxy,
		spawn::{
			LaunchSpec, MAX_CPUS, MAX_MEM_MIB, RemoteFsShare, SandboxRuntime, SandboxVm, VmonRuntime,
			VolumeMount,
		},
		vpc::{Vpc, VpcRegistry},
	},
	error::{EngineError, Result},
	home::Home,
	image::{self, CachedTemplate, TemplateBooter, TemplateRequest, TemplateSpec},
	mesh::{
		cluster_store::{ProductionStore, RollbackDisposition, SuspensionMarker},
		record::CreateRecord,
		routes::MigrationCleanupWire,
	},
	models::{
		ForkBody, MAX_FORK_CLONES, NetworkBody, PoolPutBody, RestoreBody, S3MountSpec, SandboxCreate,
	},
	net::{self, SandboxNetwork},
	pools::{PoolRegistry, WarmPool, template_key},
	portable_history::{
		PortableHistory, PortableOwnership, PortablePointInput, PortableSuspendIntent,
		RetentionPolicy,
	},
	registry::{
		LifecycleOperation, LifecyclePhase, LifecycleState, PersistencePolicy, Registry,
		SafeRuntimeIdentity, StateGeneration, TransitionBegin, TransitionDisposition, VmRecord,
	},
	s3::{S3Auth, S3Client, S3Credentials, S3MountConfig, parse_s3_uri},
	security::{
		AuditEvent, AuditLog, CREDENTIAL_GATEWAY_PORT, CredentialGateway, CredentialProvider,
		CredentialStore, EncryptedArchive, Keyring,
		credentials::{Credential, CredentialMetadata},
	},
	volumes::{self, Secret, Volume, VolumeLock},
};

const DEFAULT_SHELL_IMAGE: &str = "debian:stable-slim";
const DEFAULT_SHELL_ARGV: [&str; 3] =
	["/bin/sh", "-c", "command -v bash >/dev/null 2>&1 && exec bash -i || exec sh -i"];
const DEFAULT_CREATE_TIMEOUT_SECS: u64 = 300;
const AGENT_CONNECT_TIMEOUT: Duration = Duration::from_secs(30);
const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
const CONTROL_TIMEOUT: Duration = Duration::from_secs(10);
const LOG_FOLLOW_POLL: Duration = Duration::from_millis(100);
const EXEC_CAPTURE_CAP: Duration = Duration::from_mins(1);
const WARM_VOLUME_SLOTS: u64 = 8;
const ALLOWED_HA: [&str; 4] = ["async", "async+rerun", "off", "rerun"];
const S3_MOUNTS_FILE: &str = "s3-mounts.json";
const MAX_S3_MOUNTS: usize = 8;
const MAX_SNAPSHOT_METADATA_BYTES: u64 = 64 * 1024;

/// Single owner of the microVM registry and all VM lifecycle logic.
#[derive(Clone)]
pub struct Engine {
	inner: Arc<EngineInner>,
}
/// An active replication export pins its directory until both publication
/// cleanup and every in-flight serving stream release their references.
/// Removes a checkpoint publication that failed before an owner accepted it.
struct CheckpointCleanup {
	digest: Option<String>,
	path:   Option<PathBuf>,
}

impl CheckpointCleanup {
	const fn new(digest: String, path: PathBuf) -> Self {
		Self { digest: Some(digest), path: Some(path) }
	}

	fn disarm(&mut self) {
		self.digest = None;
		self.path = None;
	}
}

impl Drop for CheckpointCleanup {
	fn drop(&mut self) {
		let digest = self.digest.take();
		let path = self.path.take();
		if let (Some(digest), Some(path)) = (digest, path) {
			let _ = image::cas::drop_pointer_exact(&digest, &path);
			let _ = fs::remove_dir_all(path);
		}
	}
}

pub struct ReplicaExport {
	digest:       String,
	snapshot_dir: PathBuf,
	_cleanup:     CheckpointCleanup,
	object_key:   Mutex<Option<String>>,
}

impl ReplicaExport {
	pub(crate) fn path(&self) -> &Path {
		&self.snapshot_dir
	}
}

struct RuntimeOwner(Option<Runtime>);

impl RuntimeOwner {
	const fn new(runtime: Runtime) -> Self {
		Self(Some(runtime))
	}
}

impl Deref for RuntimeOwner {
	type Target = Runtime;

	fn deref(&self) -> &Self::Target {
		self.0.as_ref().expect("live engine runtime")
	}
}

impl Drop for RuntimeOwner {
	fn drop(&mut self) {
		let Some(runtime) = self.0.take() else {
			return;
		};
		if tokio::runtime::Handle::try_current().is_err() {
			drop(runtime);
			return;
		}
		thread::scope(|scope| match scope.spawn(move || drop(runtime)).join() {
			Ok(()) => {},
			Err(payload) if thread::panicking() => {
				tracing::error!("engine runtime destructor panicked during unwinding");
				drop(payload);
			},
			Err(payload) => std::panic::resume_unwind(payload),
		});
	}
}

struct LifecycleOwnership {
	handoff: Arc<dyn OwnershipHandoff>,
	owner:   String,
	epoch:   i64,
}

struct MaintenancePermit {
	inner: Arc<EngineInner>,
	id:    String,
}

impl Drop for MaintenancePermit {
	fn drop(&mut self) {
		self.inner.release_maintenance(&self.id);
	}
}

struct EngineInner {
	config: ServeConfig,
	home: Home,
	registry: Registry,
	vpcs: VpcRegistry,
	runtimes: Mutex<HashMap<String, RuntimeState>>,
	launch_cancellations: Mutex<HashMap<String, Arc<AtomicBool>>>,
	relaunch_recipes: Mutex<HashMap<String, RelaunchRecipe>>,
	pty_cache: PtyCache,
	restore_handoff: Mutex<Option<Weak<dyn OwnershipHandoff>>>,
	capture_locks: Mutex<HashMap<String, Arc<CaptureLock>>>,
	pools: PoolRegistry,
	template_memo: Mutex<HashMap<TemplateMemoKey, CachedTemplate>>,
	events: Mutex<Vec<Sender<Value>>>,
	event_sequence: AtomicU64,
	counters: Counters,
	latency: Mutex<CreateLatency>,
	net_runtime: RuntimeOwner,
	sandbox_runtime: Arc<dyn SandboxRuntime>,
	keyring: Arc<Keyring>,
	credentials: Arc<CredentialStore>,
	portable_history: Option<Arc<PortableHistory>>,
	portable_ownership: Mutex<Option<PortableOwnership>>,
	maintenance_busy: Mutex<HashSet<String>>,
	maintenance_changed: Condvar,
	maintenance_wake: Notify,
	pending_migration_staging: Mutex<HashMap<String, Arc<TransientDir>>>,
	snapshot_sources: Mutex<HashMap<String, SnapshotSource>>,
	pending_replica_exports: Mutex<HashMap<String, Arc<ReplicaExport>>>,
	portable_gc_last: Mutex<Instant>,
	#[cfg(test)]
	restore_executor: Mutex<Option<Arc<TestRestoreExecutor>>>,
	#[cfg(test)]
	capture_executor: Mutex<Option<Arc<TestCaptureExecutor>>>,
	#[cfg(test)]
	rollback_resume_executor: Mutex<Option<Arc<TestRollbackResumeExecutor>>>,
	#[cfg(test)]
	disk_resize_executor: Mutex<Option<Arc<TestDiskResizeExecutor>>>,
	audit: AuditLog,
}

impl EngineInner {
	fn release_maintenance(&self, id: &str) {
		let removed = self.maintenance_busy.lock().remove(id);
		if removed {
			self.maintenance_changed.notify_all();
		}
	}
}

#[derive(Default)]
struct RuntimeState {
	agent:             Option<AgentConn>,
	network:           Option<SandboxNetwork>,
	volume_locks:      Vec<VolumeLock>,
	encrypted_volumes: Vec<EncryptedVolumeMount>,
	secret_env:        BTreeMap<String, String>,
	env:               BTreeMap<String, String>,
	workdir:           Option<String>,

	connect_token:         Option<String>,
	network_policy:        NetworkPolicy,
	network_spec:          Option<Value>,
	timeout_stop:          Option<Sender<()>>,
	s3_proxy:              Option<S3Proxy>,
	credential_gateway:    Option<CredentialGateway>,
	snapshot_source:       Option<Arc<TransientDir>>,
	guest_activity:        Option<GuestActivity>,
	identity_complete:     bool,
	restore_volume_leases: Option<RestoreVolumeLeases>,
	/// Guest setup intentionally deferred while a mesh candidate is fenced
	/// behind durable ownership. This is internal-only: public create requests
	/// always complete setup before becoming visible.
	pending_setup:         Option<Box<CreatePlan>>,
}

/// An owned per-sandbox capture permit. Unlike a borrowed mutex guard it can
/// cross `MeshRuntime`'s async boundary while protecting the exact same gate as
/// local suspend, rollback, history, and snapshot capture paths.
pub struct LifecycleCaptureGuard {
	lock: Arc<CaptureLock>,
}

struct CaptureLock {
	held:    Mutex<bool>,
	changed: Condvar,
}

impl CaptureLock {
	fn acquire(self: &Arc<Self>) -> LifecycleCaptureGuard {
		let mut held = self.held.lock();
		while *held {
			self.changed.wait(&mut held);
		}
		*held = true;
		drop(held);
		LifecycleCaptureGuard { lock: Arc::clone(self) }
	}

	fn try_acquire(self: &Arc<Self>) -> Option<LifecycleCaptureGuard> {
		let mut held = self.held.try_lock()?;
		if *held {
			return None;
		}
		*held = true;
		drop(held);
		Some(LifecycleCaptureGuard { lock: Arc::clone(self) })
	}
}

impl Drop for LifecycleCaptureGuard {
	fn drop(&mut self) {
		let mut held = self.lock.held.lock();
		*held = false;
		self.lock.changed.notify_one();
	}
}

struct EncryptedVolumeMount {
	name:      String,
	mount_dir: PathBuf,
	slot_dir:  PathBuf,
	archive:   PathBuf,
	key_id:    String,
	read_only: bool,
	sealed:    bool,
	preserve:  bool,
}

impl EncryptedVolumeMount {
	const fn arm(&mut self) {
		self.preserve = true;
	}

	fn seal(&mut self, keyring: &Keyring) -> Result<()> {
		if self.read_only {
			self.sealed = true;
			self.preserve = false;
			return Ok(());
		}
		if self.sealed {
			return Ok(());
		}
		match EncryptedArchive::seal(&self.mount_dir, &self.archive, keyring, &self.key_id) {
			Ok(()) => {
				self.sealed = true;
				self.preserve = false;
				Ok(())
			},
			Err(error) => {
				self.preserve = true;
				Err(error)
			},
		}
	}
}

impl Drop for EncryptedVolumeMount {
	fn drop(&mut self) {
		if !self.preserve {
			let _ = fs::remove_dir_all(&self.slot_dir);
		}
	}
}

struct TransientDir(PathBuf);

impl Drop for TransientDir {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.0);
	}
}
/// Removes a production staging archive unless publication completed. Local
/// recovery points deliberately keep their archive as the recovery authority.
struct ArchiveCleanup {
	path:     PathBuf,
	preserve: bool,
}

impl Drop for ArchiveCleanup {
	fn drop(&mut self) {
		if !self.preserve {
			let _ = fs::remove_file(&self.path);
		}
	}
}
/// Fresh distributed writable-volume votes held until the portable
/// replacement is durably committed.  Failure paths drop the guard only
/// after removing any candidate, so no new VM can outlive its votes.
struct RestoreVolumeLeases {
	handoff: Arc<dyn OwnershipHandoff>,
	runtime: tokio::runtime::Handle,
	leases:  Option<Vec<Value>>,
}

impl RestoreVolumeLeases {
	fn acquire(
		runtime: tokio::runtime::Handle,
		handoff: Arc<dyn OwnershipHandoff>,
		sid: &str,
		owner: &str,
		epoch: i64,
		params: &mut Map<String, Value>,
	) -> Result<Self> {
		let leases =
			runtime.block_on(handoff.acquire_restore_volume_leases(sid, owner, epoch, params))?;
		Ok(Self { handoff, runtime, leases: Some(leases) })
	}

	fn persist(&self, sid: &str) -> Result<()> {
		if let Some(leases) = self.leases.as_deref() {
			self
				.runtime
				.block_on(self.handoff.persist_restore_volume_leases(sid, leases))?;
		}
		Ok(())
	}

	fn disarm(&mut self) {
		self.leases = None;
	}
}

impl Drop for RestoreVolumeLeases {
	fn drop(&mut self) {
		if let Some(leases) = self.leases.take() {
			let _ = self
				.runtime
				.block_on(self.handoff.release_restore_volume_leases(leases));
		}
	}
}
/// Removes an uncommitted rollback-journal temp file and persists the
/// directory removal so a crash cannot turn write residue into replay input.
struct JournalTemp(PathBuf);

impl JournalTemp {
	fn disarm(&mut self) {
		self.0.clear();
	}
}

impl Drop for JournalTemp {
	fn drop(&mut self) {
		if self.0.as_os_str().is_empty() {
			return;
		}
		if fs::remove_file(&self.0).is_ok()
			&& let Some(parent) = self.0.parent()
		{
			let _ = OpenOptions::new()
				.read(true)
				.open(parent)
				.and_then(|file| file.sync_all());
		}
	}
}

#[derive(Clone)]
struct SnapshotSource {
	path:  PathBuf,
	guard: Option<Arc<TransientDir>>,
}

/// Releases a suspend's frozen source if any capture/publication step fails.
/// A successful suspend disarms it immediately before lifecycle teardown takes
/// ownership of the paused VM.
struct ResumeOnError {
	vm:              SandboxVm,
	source_pid:      Option<i64>,
	armed:           bool,
	#[cfg(test)]
	resume_executor: Option<Arc<TestRollbackResumeExecutor>>,
}

impl ResumeOnError {
	fn new(vm: SandboxVm) -> Self {
		Self {
			source_pid: vm.pid().ok().flatten().map(i64::from),
			vm,
			armed: true,
			#[cfg(test)]
			resume_executor: None,
		}
	}

	#[cfg(test)]
	fn with_resume_executor(
		vm: SandboxVm,
		resume_executor: Option<Arc<TestRollbackResumeExecutor>>,
	) -> Self {
		let mut guard = Self::new(vm);
		guard.resume_executor = resume_executor;
		guard
	}

	const fn disarm(&mut self) {
		self.armed = false;
	}
}

impl Drop for ResumeOnError {
	fn drop(&mut self) {
		if !self.armed {
			return;
		}
		let Some(source_pid) = self.source_pid else {
			return;
		};
		if self.vm.pid().ok().flatten().map(i64::from) != Some(source_pid) {
			return;
		}
		#[cfg(test)]
		if let Some(resume) = &self.resume_executor {
			resume(&self.vm);
			return;
		}
		if let Ok(mut control) = control_for_vm(&self.vm) {
			let _ = control.resume();
		}
	}
}

#[derive(Default, Clone)]
struct NetworkPolicy {
	block_network:          Option<bool>,
	egress_allow:           Option<Vec<String>>,
	egress_allow_domains:   Option<Vec<String>>,
	inbound_cidr_allowlist: Option<Vec<String>>,
}

#[derive(Default)]
struct Counters {
	created:     AtomicU64,
	terminated:  AtomicU64,
	idle_reaped: AtomicU64,
	exec:        AtomicU64,
	file_read:   AtomicU64,
	file_write:  AtomicU64,
	file_delete: AtomicU64,
	snapshot:    AtomicU64,
	auth_failed: AtomicU64,
}

#[derive(Default)]
struct CreateLatency {
	sum_ms: f64,
	count:  u64,
}

struct EngineExecControl {
	handle: ExecHandle,
}

struct EnginePtyControl {
	handle: PtyAgentHandle,
}

#[derive(Clone)]
struct RelaunchRecipe {
	params:       SandboxCreate,
	template_dir: PathBuf,
	image_spec:   Option<image::ImageConfig>,
	image_ref:    Option<String>,
}

struct CreatePlan {
	params:               SandboxCreate,
	sid:                  String,
	ha:                   String,
	restart_policy:       String,
	tags:                 HashMap<String, String>,
	secrets:              Vec<Secret>,
	secret_env:           BTreeMap<String, String>,
	timeout_secs:         Option<u64>,
	volume_specs:         Vec<ResolvedVolume>,
	s3_specs:             Vec<ResolvedS3Mount>,
	template_dir:         PathBuf,
	image_spec:           Option<image::ImageConfig>,
	image_ref:            Option<String>,
	pool_key:             String,
	warm_volumes:         bool,
	host_slot:            bool,
	networked_warm:       bool,
	networked_warm_linux: bool,
	relaunch_params:      SandboxCreate,
	retained_rootfs:      bool,
}

/// Identity of a memoized image-template resolution. The boot-verify timeout
/// is deliberately excluded: it never changes which template is produced.
/// Dockerfile builds are never memoized — their inputs live outside the key.
#[derive(Clone, Eq, Hash, PartialEq)]
struct TemplateMemoKey {
	image:     Option<String>,
	disk_mb:   u64,
	memory:    u64,
	cpus:      u64,
	fs_slots:  u64,
	host_slot: bool,
	nic_slot:  bool,
	tap_slot:  bool,
}
#[cfg(test)]
type TestRestoreExecutor = dyn Fn(&Engine, Map<String, Value>, &Path, bool, Option<Arc<AtomicBool>>) -> Result<Value>
	+ Send
	+ Sync;
#[cfg(test)]
type TestCaptureExecutor =
	dyn Fn(&Engine, &str, &str, bool, bool) -> Result<RecoveryPoint> + Send + Sync;
#[cfg(test)]
type TestRollbackResumeExecutor = dyn Fn(&SandboxVm) + Send + Sync;
#[cfg(test)]
type TestDiskResizeExecutor = dyn Fn(&Path, &Path, u64) -> Result<()> + Send + Sync;
#[derive(Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct SnapshotOptions {
	agent: Option<bool>,
	block_network: Option<bool>,
	env: Option<HashMap<String, String>>,
	workdir: Option<String>,
	tags: Option<HashMap<String, String>>,
	timeout: Option<f64>,
	timeout_secs: Option<u64>,
	idle_timeout_secs: Option<f64>,
	activity_threshold_bytes: Option<u64>,
	persistence: Option<PersistencePolicy>,
	readiness_probe: Option<Value>,
	secrets: Option<Vec<Value>>,
	s3_mounts: Option<HashMap<String, S3MountSpec>>,
	command: Option<Vec<String>>,
	credentials: Option<Vec<String>>,
	owner_tenant: Option<String>,
	encryption_key_id: Option<String>,
	ports: Option<Vec<u16>>,
	egress_allow: Option<Vec<String>>,
	egress_allow_domains: Option<Vec<String>>,
	inbound_cidr_allowlist: Option<Vec<String>>,
}

#[derive(Clone)]
struct ResolvedSnapshotOptions {
	agent: bool,
	block_network: Option<bool>,
	env: BTreeMap<String, String>,
	secret_env: BTreeMap<String, String>,
	secret_names: Vec<String>,
	workdir: Option<String>,
	tags: HashMap<String, String>,
	timeout_secs: Option<u64>,
	idle_timeout_secs: Option<f64>,
	activity_threshold_bytes: Option<u64>,
	persistence: PersistencePolicy,
	readiness_probe: Option<Value>,
	s3_mounts: Option<HashMap<String, S3MountSpec>>,
	command: Option<Vec<String>>,
	credentials: Vec<String>,
	owner_tenant: String,
	encryption_key_id: String,
	ports: Option<Vec<u16>>,
	egress_allow: Option<Vec<String>>,
	egress_allow_domains: Option<Vec<String>>,
	inbound_cidr_allowlist: Option<Vec<String>>,
}

#[derive(Clone, Copy)]
enum SnapshotLaunchMode {
	Restore,
	Fork,
}

struct ResolvedVolume {
	mountpoint: String,
	name:       String,
	tag:        String,
	host_dir:   PathBuf,
	read_only:  bool,
	lock:       Option<VolumeLock>,
	encrypted:  Option<EncryptedVolumeMount>,
}

struct ResolvedS3Mount {
	mountpoint: String,
	tag:        String,
	read_only:  bool,
	client:     Arc<S3Client>,
	meta:       Value,
}
/// A rollback is destructive only after this journal and its safety point are
/// durable outside the sandbox directory.  It deliberately contains no
/// credentials or environment values.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct RollbackJournal {
	sandbox_id:            String,
	target_recovery_point: String,
	safety_recovery_point: String,
	generation:            u64,
	checkpoint_generation: u64,
	source_token:          String,
	portable_owner:        String,
	portable_owner_epoch:  i64,
}

fn rollback_journal_matches_marker(
	journal: &RollbackJournal,
	sandbox_id: &str,
	owner: &str,
	epoch: i64,
	operation_generation: u64,
) -> bool {
	journal.sandbox_id == sandbox_id
		&& journal.portable_owner == owner
		&& journal.portable_owner_epoch == epoch
		&& journal.generation == operation_generation
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RollbackReplayDecision {
	AbortRetainSource,
	FinalizeRetainTarget,
	RecoverSafety,
}

const fn rollback_replay_decision(
	source_is_live: bool,
	local_candidate_is_running: bool,
	disposition: Option<&RollbackDisposition>,
) -> RollbackReplayDecision {
	if source_is_live {
		RollbackReplayDecision::AbortRetainSource
	} else if local_candidate_is_running
		&& matches!(disposition, Some(RollbackDisposition::CommittedRunning))
	{
		RollbackReplayDecision::FinalizeRetainTarget
	} else {
		RollbackReplayDecision::RecoverSafety
	}
}

fn launch_spec_for_cluster(mode: ClusterMode, spec: &LaunchSpec) -> LaunchSpec {
	match mode {
		ClusterMode::Production => spec.clone().with_owner_lease_secs(15),
		ClusterMode::SingleNode => spec.clone(),
	}
}

impl Engine {
	/// Construct the real engine with the built-in `vmon vmm` runtime.
	pub fn new(config: ServeConfig) -> Result<Self> {
		Self::with_runtime(config, Arc::new(VmonRuntime))
	}

	/// Construct the engine with an explicitly selected sandbox runtime.
	pub fn with_runtime(
		config: ServeConfig,
		sandbox_runtime: Arc<dyn SandboxRuntime>,
	) -> Result<Self> {
		align_process_home(&config.home);
		let home = Home::new(config.home.clone());
		fs::create_dir_all(home.vms_dir())?;
		fs::create_dir_all(home.templates_dir())?;
		fs::create_dir_all(home.volumes_dir())?;
		let keyring = Arc::new(Keyring::open(&home)?);
		let portable_history = PortableHistory::connect(&config, keyring.as_ref())?;
		let credentials = match config.cluster_mode {
			ClusterMode::SingleNode => Arc::new(CredentialStore::open(&home, Arc::clone(&keyring))?),
			ClusterMode::Production => {
				let postgres_url = config.postgres_url.as_deref().ok_or_else(|| {
					EngineError::invalid("production credentials require postgres_url")
				})?;
				let fallback_key = config.portable_history_key_id.as_deref().ok_or_else(|| {
					EngineError::invalid("production credentials require portable_history_key_id")
				})?;
				Arc::new(CredentialStore::open_production(
					Arc::new(ProductionStore::connect(postgres_url)?),
					Arc::clone(&keyring),
					fallback_key,
				)?)
			},
		};
		let audit = AuditLog::open(&home)?;
		let registry = Registry::new();
		let lock_requests = registry.rehydrate(&home)?;
		registry.rebuild_idempotency_index();
		let vpcs = VpcRegistry::open(home.root())?;
		let net_runtime = Runtime::new()
			.map_err(|err| EngineError::engine(format!("starting network runtime: {err}")))?;
		let engine = Self {
			inner: Arc::new(EngineInner {
				config,
				home,
				registry,
				vpcs,
				runtimes: Mutex::new(HashMap::new()),
				relaunch_recipes: Mutex::new(HashMap::new()),
				pty_cache: PtyCache::default(),
				launch_cancellations: Mutex::new(HashMap::new()),
				capture_locks: Mutex::new(HashMap::new()),
				restore_handoff: Mutex::new(None),
				#[cfg(test)]
				restore_executor: Mutex::new(None),
				#[cfg(test)]
				capture_executor: Mutex::new(None),
				#[cfg(test)]
				rollback_resume_executor: Mutex::new(None),
				#[cfg(test)]
				disk_resize_executor: Mutex::new(None),
				pools: PoolRegistry::new(),
				template_memo: Mutex::new(HashMap::new()),
				events: Mutex::new(Vec::new()),
				event_sequence: AtomicU64::new(0),
				counters: Counters::default(),
				latency: Mutex::new(CreateLatency::default()),
				net_runtime: RuntimeOwner::new(net_runtime),
				sandbox_runtime,
				keyring,
				credentials,
				audit,
				maintenance_busy: Mutex::new(HashSet::new()),
				maintenance_changed: Condvar::new(),
				maintenance_wake: Notify::new(),
				snapshot_sources: Mutex::new(HashMap::new()),
				portable_gc_last: Mutex::new(
					Instant::now()
						.checked_sub(Duration::from_mins(1))
						.unwrap_or_else(Instant::now),
				),
				portable_history,
				portable_ownership: Mutex::new(None),
				pending_migration_staging: Mutex::new(HashMap::new()),
				pending_replica_exports: Mutex::new(HashMap::new()),
			}),
		};
		engine.reacquire_volume_locks(lock_requests)?;
		engine.recover_orphaned_encrypted_volumes()?;
		if engine.inner.portable_history.is_none() {
			// A standalone Engine has no MeshRuntime handoff to wait for, so
			// preserve its original restart recovery guarantees.
			engine.recover_rollback_journals()?;
			engine.reconcile_rollback_pins()?;
		}
		crate::security::crypto::cleanup_stale_temporary_files(&engine.home().security_dir())?;
		reclaim_orphaned_disk_artifacts(engine.home())?;
		let runtime_root = engine.home().security_dir().join("runtime");
		let preserve_paths = engine.staging_preserve_paths();
		crate::engine::staging_gc::reclaim_orphaned_staging(&runtime_root, &preserve_paths)?;
		engine.rehydrate_runtime_identities()?;
		if engine.inner.portable_history.is_none() {
			engine.reconcile_lifecycle_transitions();
		}
		engine.start_configured_pools()?;
		Ok(engine)
	}

	/// Attach mesh routing ownership adoption after `MeshRuntime` is
	/// constructed. The callback is crate-internal: HTTP clients cannot observe
	/// staging.
	pub(crate) fn set_restore_handoff(&self, handoff: Arc<dyn OwnershipHandoff>) {
		*self.inner.restore_handoff.lock() = Some(Arc::downgrade(&handoff));
		// Durable suspend/rollback markers are non-serving until this
		// ownership callback is installed. Reconcile only afterwards, so a
		// fresh Engine cannot pause, resume, or claim them prematurely.
		if let Err(error) = self.recover_rollback_journals() {
			tracing::warn!(%error, "rollback journal reconciliation failed after ownership handoff installation");
		}
		if let Err(error) = self.reconcile_rollback_pins() {
			tracing::warn!(%error, "rollback pin reconciliation failed after ownership handoff installation");
		}
		self.reconcile_lifecycle_transitions();
	}

	fn lifecycle_handoff_owner(&self, id: &str) -> Result<Option<LifecycleOwnership>> {
		let handoff = self
			.inner
			.restore_handoff
			.lock()
			.as_ref()
			.and_then(Weak::upgrade);
		if self.inner.portable_history.is_some() {
			let handoff = handoff.ok_or_else(|| {
				EngineError::engine("production lifecycle requires an ownership handoff")
			})?;
			let mut ownership = self.inner.portable_ownership.lock();
			if ownership.is_none() {
				*ownership = PortableOwnership::connect(&self.inner.config)?;
			}
			let (owner, epoch) = ownership
				.as_ref()
				.ok_or_else(|| EngineError::engine("production ownership bridge disappeared"))?
				.current(id)?;
			return Ok(Some(LifecycleOwnership { handoff, owner, epoch }));
		}
		let Some(handoff) = handoff else {
			return Ok(None);
		};
		let Some((owner, epoch)) = handoff.current_owner(id)? else {
			return Ok(None);
		};
		Ok(Some(LifecycleOwnership { handoff, owner, epoch }))
	}

	/// Persisted records can reference a live restore/capture source across a
	/// daemon restart.  Preserve only paths which the staging GC later
	/// canonicalizes beneath the managed runtime root.
	fn staging_preserve_paths(&self) -> Vec<PathBuf> {
		self
			.inner
			.registry
			.list()
			.into_iter()
			.filter(|record| record.status == "running" || !record.lifecycle.is_converged())
			.flat_map(|record| {
				[record.runtime_identity.source, record.runtime_identity.template, record.source]
			})
			.flatten()
			.map(PathBuf::from)
			.collect()
	}

	fn capture_lock(&self, id: &str) -> Arc<CaptureLock> {
		let mut locks = self.inner.capture_locks.lock();
		// The map owns one reference. Retain only active/waiting permits, whose
		// guards or callers hold another reference, so historical sandbox IDs
		// do not turn this coordination table into an unbounded cache.
		locks.retain(|_, lock| Arc::strong_count(lock) > 1);
		locks
			.entry(id.to_owned())
			.or_insert_with(|| {
				Arc::new(CaptureLock { held: Mutex::new(false), changed: Condvar::new() })
			})
			.clone()
	}

	/// Attempt to acquire the shared lifecycle/capture permit without blocking
	/// an async mesh request. `Ok(None)` means another capture or lifecycle
	/// operation owns this sandbox; callers must retry rather than overlap it.
	pub(crate) fn try_acquire_running_capture(
		&self,
		id: &str,
	) -> Result<Option<LifecycleCaptureGuard>> {
		let Some(guard) = self.capture_lock(id).try_acquire() else {
			return Ok(None);
		};
		let record = self.get_record(id, false)?;
		if record.status != "running" || !record.lifecycle.is_converged() {
			return Err(EngineError::busy("sandbox lifecycle is not steady for capture"));
		}
		Ok(Some(guard))
	}

	/// Stop pool refillers and parked clones. User sandboxes are intentionally
	/// left running.
	pub fn shutdown(&self) {
		self.inner.pools.shutdown();
	}

	/// Run guest-activity sampling, recovery capture, and idle suspension until
	/// shutdown.
	pub fn start_maintenance(
		self: &Arc<Self>,
		mut shutdown: broadcast::Receiver<()>,
	) -> JoinHandle<()> {
		let engine = Arc::clone(self);
		tokio::spawn(async move {
			let mut next_run = tokio::time::Instant::now();
			loop {
				tokio::select! {
					() = tokio::time::sleep_until(next_run) => {
						let worker = Arc::clone(&engine);
						if let Err(error) =
							tokio::task::spawn_blocking(move || worker.maintenance_once()).await
						{
							tracing::warn!("sandbox maintenance task failed: {error}");
						}
						next_run = tokio::time::Instant::now() + engine.maintenance_interval();
					},
					() = engine.inner.maintenance_wake.notified() => {
						let candidate =
							tokio::time::Instant::now() + engine.maintenance_interval();
						next_run = next_run.min(candidate);
					},
					_ = shutdown.recv() => break,
				}
			}
		})
	}

	fn maintenance_interval(&self) -> Duration {
		let default_idle_timeout = self.inner.config.idle_timeout;
		let active_idle_timeouts = self.inner.registry.list().into_iter().filter_map(|record| {
			(record.status == "running").then(|| {
				record
					.detail
					.get("idle_timeout_secs")
					.and_then(Value::as_f64)
					.unwrap_or(default_idle_timeout)
			})
		});
		active_idle_timeouts
			.chain([self.inner.config.history_disk_sec, self.inner.config.history_checkpoint_sec])
			.filter(|seconds| seconds.is_finite() && *seconds > 0.0)
			.map(|seconds| Duration::from_secs_f64((seconds / 4.0).clamp(1.0, 30.0)))
			.min()
			.unwrap_or(Duration::from_secs(30))
	}

	fn wake_maintenance(&self) {
		self.inner.maintenance_wake.notify_one();
	}

	fn maintenance_permit(&self, id: &str) -> MaintenancePermit {
		let id = id.to_owned();
		let mut busy = self.inner.maintenance_busy.lock();
		while busy.contains(&id) {
			self.inner.maintenance_changed.wait(&mut busy);
		}
		busy.insert(id.clone());
		drop(busy);
		MaintenancePermit { inner: Arc::clone(&self.inner), id }
	}

	fn maintenance_once(&self) {
		self.reconcile_lifecycle_transitions();
		if let Err(error) = self.reconcile_rollback_pins() {
			tracing::warn!(%error, "rollback pin reconciliation failed");
		}
		if let Some(history) = &self.inner.portable_history {
			let should_gc = {
				let mut last = self.inner.portable_gc_last.lock();
				if last.elapsed() >= Duration::from_mins(1) {
					*last = Instant::now();
					true
				} else {
					false
				}
			};
			if should_gc && let Err(error) = history.gc() {
				tracing::warn!(%error, "portable history garbage collection failed");
			}
		}
		for record in self.inner.registry.list() {
			if record.status != "running" {
				continue;
			}
			if !self.inner.maintenance_busy.lock().insert(record.id.clone()) {
				continue;
			}
			if let Err(error) = self.maintain_sandbox(&record) {
				tracing::warn!(sandbox = record.id, %error, "sandbox maintenance failed");
			}
			self.inner.release_maintenance(&record.id);
		}
		if let Err(error) = self.enforce_storage_quota() {
			tracing::warn!(%error, "sandbox storage quota enforcement failed");
		}
	}

	fn enforce_storage_quota(&self) -> Result<()> {
		let quota_mb = self.inner.config.storage_quota_mb;
		if quota_mb == 0 {
			return Ok(());
		}
		let quota = quota_mb.saturating_mul(1024 * 1024);
		let mut total = 0_u64;
		let mut eligible = Vec::new();
		for record in self.inner.registry.list() {
			if record.status == "running" || !record.lifecycle.is_converged() {
				continue;
			}
			let size = self.stored_state_size(&record)?;
			total = total.saturating_add(size);
			let eviction_key = match &record.persistence {
				PersistencePolicy::Ephemeral => Some((0_u8, 0_u8)),
				PersistencePolicy::Sticky { priority } => Some((1_u8, *priority)),
				PersistencePolicy::Persistent => None,
			};
			if let Some((kind, priority)) = eviction_key {
				eligible.push((kind, priority, record.created_at, record, size));
			}
		}
		eligible.sort_by(|left, right| {
			left
				.0
				.cmp(&right.0)
				.then(left.1.cmp(&right.1))
				.then_with(|| left.2.total_cmp(&right.2))
		});
		for (_, _, _, record, size) in eligible {
			if total <= quota {
				break;
			}
			let transition =
				self.begin_state_transition(&record.id, LifecyclePhase::Unknown("lost".to_owned()))?;
			if transition.disposition != TransitionDisposition::Acquired {
				continue;
			}
			if let Err(error) = self.discard_stored_state(&record) {
				self.fail_state_transition(&record.id, transition.generation, &error);
				return Err(error);
			}
			if let Err(error) =
				self
					.inner
					.registry
					.update_detail_persisted(self.home(), &record.id, |detail| {
						detail.insert("reason".to_owned(), json!("evicted"));
					}) {
				self.fail_state_transition(&record.id, transition.generation, &error);
				return Err(error);
			}
			self.complete_state_transition(
				&record.id,
				transition.generation,
				LifecyclePhase::Unknown("lost".to_owned()),
			)?;
			total = total.saturating_sub(size);
			if let Some(updated) = self.inner.registry.get(&record.id) {
				self.publish_record_event("lost", &updated);
			}
		}
		Ok(())
	}

	fn stored_state_size(&self, record: &VmRecord) -> Result<u64> {
		Ok(path_size(&self.home().vm_dir(&record.name))?
			.saturating_add(path_size(&self.recovery_root(&record.id)?)?))
	}

	fn discard_stored_state(&self, record: &VmRecord) -> Result<()> {
		let vm_dir = self.home().vm_dir(&record.name);
		match fs::read_dir(&vm_dir) {
			Ok(entries) => {
				for entry in entries {
					let entry = entry?;
					if entry.file_name() == "meta.json" {
						continue;
					}
					remove_path(&entry.path())?;
				}
			},
			Err(error) if error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error.into()),
		}
		self.delete_local_recovery_history(&record.id)
	}

	fn apply_ephemeral_discard(&self, record: &VmRecord) -> Result<()> {
		if record.persistence != PersistencePolicy::Ephemeral {
			return Ok(());
		}
		self.discard_stored_state(record)?;
		self
			.inner
			.registry
			.update_detail_persisted(self.home(), &record.id, |detail| {
				detail.insert("state_discarded".to_owned(), json!(true));
				detail.insert("persistence".to_owned(), json!(PersistencePolicy::Ephemeral));
			})?;
		Ok(())
	}

	fn reconcile_lifecycle_transitions(&self) {
		if self.inner.portable_history.is_some()
			&& self
				.inner
				.restore_handoff
				.lock()
				.as_ref()
				.and_then(Weak::upgrade)
				.is_none()
		{
			return;
		}
		for input in self.inner.registry.startup_reconciliation_inputs() {
			let id = input.record.id.clone();
			if !self.inner.maintenance_busy.lock().insert(id.clone()) {
				continue;
			}
			if let Err(error) =
				self.reconcile_lifecycle_transition(input.record, input.lifecycle, input.pid_alive)
			{
				tracing::warn!(%error, "sandbox lifecycle reconciliation failed");
			}
			self.inner.release_maintenance(&id);
		}
		if let Err(error) = self.reconcile_unplaced_rollback_markers() {
			tracing::warn!(%error, "unplaced portable rollback reconciliation failed");
		}
		if let Err(error) = self.reconcile_unplaced_suspend_markers() {
			tracing::warn!(%error, "unplaced portable suspend reconciliation failed");
		}
	}

	fn reconcile_unplaced_rollback_markers(&self) -> Result<()> {
		let portable = {
			let mut ownership = self.inner.portable_ownership.lock();
			if ownership.is_none() {
				*ownership = PortableOwnership::connect(&self.inner.config)?;
			}
			let Some(portable) = ownership.as_ref().cloned() else {
				return Ok(());
			};
			portable
		};
		for marker in portable.rollback_markers()? {
			// Only the original, still-live source of the exact rollback may
			// keep this marker from being claimed. A stale placeholder from a
			// former owner must not permanently block orphan recovery.
			if let Some(candidate) = self.inner.registry.get(&marker.sid) {
				let journal = self
					.rollback_journal_path(&marker.sid)
					.ok()
					.and_then(|path| fs::read(path).ok())
					.and_then(|bytes| serde_json::from_slice::<RollbackJournal>(&bytes).ok());
				let exact_live_source = journal.is_some_and(|journal| {
					rollback_journal_matches_marker(
						&journal,
						&marker.sid,
						&marker.owner,
						marker.epoch,
						marker.operation_generation,
					) && candidate.status == "running"
						&& candidate.lifecycle.generation.0 == marker.operation_generation
						&& matches!(
							candidate.lifecycle.operation.as_ref(),
							Some(LifecycleOperation::Rollback { .. })
						) && candidate
						.detail
						.get("rollback_source_token")
						.and_then(Value::as_str)
						.is_some_and(|token| token == journal.source_token)
						&& self
							.sandbox_is_running(&self.sandbox(&marker.sid))
							.unwrap_or(false)
				});
				if exact_live_source {
					if let Err(error) = portable.adopt_live_rollback(
						&marker.sid,
						marker.epoch,
						marker.operation_generation,
					) {
						tracing::debug!(sandbox = %marker.sid, %error, "live rollback marker adoption deferred");
					}
					continue;
				}
				if let Err(error) = self.teardown(&candidate) {
					tracing::warn!(sandbox = %marker.sid, %error, "unable to fence stale local rollback placeholder");
					continue;
				}
				self.inner.registry.remove(&marker.sid);
			}
			let reconcile_result = (|| -> Result<()> {
				let capture_lock = self.capture_lock(&marker.sid);
				let _capture_guard = capture_lock.acquire();
				let handoff = self
					.inner
					.restore_handoff
					.lock()
					.as_ref()
					.and_then(Weak::upgrade)
					.ok_or_else(|| {
						EngineError::engine("portable rollback marker lacks ownership handoff")
					})?;
				let lease = portable.claim_expected(
					&marker.sid,
					&marker.owner,
					marker.epoch,
					marker.operation_generation,
				)?;
				if let Err(error) = handoff.begin_restore(&marker.sid, &lease.owner_node, lease.epoch) {
					let _ = portable.abort(&lease);
					return Err(error);
				}
				let heartbeat = match portable.start_restore_heartbeat(&lease) {
					Ok(heartbeat) => heartbeat,
					Err(error) => {
						let _ = handoff.abort_restore(&marker.sid, &lease.owner_node, lease.epoch);
						let _ = portable.abort(&lease);
						return Err(error);
					},
				};
				self
					.inner
					.launch_cancellations
					.lock()
					.insert(marker.sid.clone(), heartbeat.lost_signal());
				let restore = (|| -> Result<()> {
					let (source, mut params) = self.open_recovery(&marker.sid, &marker.safety)?;
					params.insert("name".to_owned(), json!(marker.sid));
					params
						.insert("checkpoint_generation".to_owned(), json!(marker.checkpoint_generation));
					ensure_checkpoint_template_present(&source.path)?;
					self.restore_from_template(params, &source.path, false)?;
					if let Some(guard) = source.guard {
						self
							.inner
							.runtimes
							.lock()
							.entry(marker.sid.clone())
							.or_default()
							.snapshot_source = Some(guard);
					}
					Ok(())
				})();
				self.inner.launch_cancellations.lock().remove(&marker.sid);
				let authorized = self.inner.net_runtime.block_on(heartbeat.finish());
				if let Err(error) = restore {
					if let Some(candidate) = self.inner.registry.get(&marker.sid) {
						let _ = self.teardown(&candidate);
						self.inner.registry.remove(&marker.sid);
					}
					let _ = handoff.abort_restore(&marker.sid, &lease.owner_node, lease.epoch);
					let _ = portable.abort(&lease);
					return Err(error);
				}
				match authorized {
					Ok(true) => {},
					Ok(false) => {
						if let Some(candidate) = self.inner.registry.get(&marker.sid) {
							let _ = self.teardown(&candidate);
							self.inner.registry.remove(&marker.sid);
						}
						let _ = handoff.abort_restore(&marker.sid, &lease.owner_node, lease.epoch);
						let _ = portable.abort(&lease);
						return Err(EngineError::busy(
							"portable rollback lease was lost during recovery",
						));
					},
					Err(error) => {
						if let Some(candidate) = self.inner.registry.get(&marker.sid) {
							let _ = self.teardown(&candidate);
							self.inner.registry.remove(&marker.sid);
						}
						let _ = handoff.abort_restore(&marker.sid, &lease.owner_node, lease.epoch);
						let _ = portable.abort(&lease);
						return Err(error);
					},
				}
				if let Err(error) = handoff.commit_rollback(&marker.sid, &lease.owner_node, lease.epoch)
					&& handoff
						.commit_rollback(&marker.sid, &lease.owner_node, lease.epoch)
						.is_err()
				{
					return Err(error);
				}
				Ok(())
			})();
			if let Err(error) = reconcile_result {
				tracing::debug!(sandbox = %marker.sid, %error, "portable rollback marker reconciliation deferred");
			}
		}
		Ok(())
	}

	fn reconcile_unplaced_suspend_markers(&self) -> Result<()> {
		let portable = {
			let mut ownership = self.inner.portable_ownership.lock();
			if ownership.is_none() {
				*ownership = PortableOwnership::connect(&self.inner.config)?;
			}
			let Some(portable) = ownership.as_ref().cloned() else {
				return Ok(());
			};
			portable
		};
		for marker in portable.suspend_markers()? {
			if self.inner.registry.get(&marker.sid).is_some() {
				continue;
			}
			if marker.state == "resuming" {
				let result = (|| -> Result<()> {
					let handoff = self
						.inner
						.restore_handoff
						.lock()
						.as_ref()
						.and_then(Weak::upgrade)
						.ok_or_else(|| {
							EngineError::engine("portable resume marker lacks ownership handoff")
						})?;
					let claimed_identity = portable.claim_suspend_marker(&marker)?;
					let lease = claimed_identity.lease;
					let claimed = portable
						.suspend_marker(&marker.sid)?
						.ok_or_else(|| EngineError::engine("resuming marker disappeared after claim"))?;
					if claimed.state != "resuming"
						|| claimed.point != marker.point
						|| claimed.generation != marker.generation
						|| claimed.owner != lease.owner_node
						|| claimed.epoch != lease.epoch
					{
						let _ = portable.abort(&lease);
						return Err(EngineError::busy("resuming marker changed while being recovered"));
					}
					// The placeholder is non-serving and supplies only durable,
					// non-secret identity to the exact recovery restore.
					self.mesh_install_suspended_placeholder(&claimed_identity.record, &claimed)?;
					if let Err(error) =
						handoff.begin_restore(&marker.sid, &lease.owner_node, lease.epoch)
					{
						let _ = self.inner.registry.remove(&marker.sid);
						let _ = portable.abort(&lease);
						return Err(error);
					}
					let heartbeat = match portable.start_restore_heartbeat(&lease) {
						Ok(heartbeat) => heartbeat,
						Err(error) => {
							let _ = handoff.abort_restore(&marker.sid, &lease.owner_node, lease.epoch);
							let _ = self.inner.registry.remove(&marker.sid);
							let _ = portable.abort(&lease);
							return Err(error);
						},
					};
					self
						.inner
						.launch_cancellations
						.lock()
						.insert(marker.sid.clone(), heartbeat.lost_signal());
					let restored = self.restore_recovery_identity(
						&marker.sid,
						&marker.point,
						Some((Arc::clone(&handoff), lease.owner_node.clone(), lease.epoch)),
					);
					self.inner.launch_cancellations.lock().remove(&marker.sid);
					let authorization_error = match self.inner.net_runtime.block_on(heartbeat.finish()) {
						Ok(true) => None,
						Ok(false) => {
							Some(EngineError::busy("resuming marker lease was lost during recovery"))
						},
						Err(error) => Some(error),
					};
					if let Err(error) = restored {
						if let Some(candidate) = self.inner.registry.get(&marker.sid) {
							let _ = self.teardown(&candidate);
							self.inner.registry.remove(&marker.sid);
						}
						let _ = handoff.abort_restore(&marker.sid, &lease.owner_node, lease.epoch);
						let _ = portable.abort(&lease);
						return Err(error);
					}
					if let Some(error) = authorization_error {
						if let Some(candidate) = self.inner.registry.get(&marker.sid) {
							let _ = self.teardown(&candidate);
							self.inner.registry.remove(&marker.sid);
						}
						let _ = handoff.abort_restore(&marker.sid, &lease.owner_node, lease.epoch);
						let _ = portable.abort(&lease);
						return Err(error);
					}
					if let Err(error) =
						handoff.commit_restore(&marker.sid, &lease.owner_node, lease.epoch)
						&& handoff
							.commit_restore(&marker.sid, &lease.owner_node, lease.epoch)
							.is_err()
					{
						// The first commit may already have atomically exposed
						// Running. Retain the candidate, lease guard, and
						// restoring marker for fenced reconciliation.
						return Err(error);
					}
					self.disarm_restore_volume_leases(&marker.sid);
					Ok(())
				})();
				if let Err(error) = result {
					tracing::debug!(sandbox = %marker.sid, %error, "portable resuming marker reconciliation deferred");
				}
				continue;
			}
			if marker.state != "suspending" {
				continue;
			}
			let result = (|| -> Result<()> {
				let handoff = self
					.inner
					.restore_handoff
					.lock()
					.as_ref()
					.and_then(Weak::upgrade)
					.ok_or_else(|| {
						EngineError::engine("portable suspend marker lacks ownership handoff")
					})?;
				let claimed_identity = portable.claim_suspend_marker(&marker)?;
				let lease = claimed_identity.lease;
				let claimed = portable
					.suspend_marker(&marker.sid)?
					.ok_or_else(|| EngineError::engine("suspending marker disappeared after claim"))?;
				if claimed.state != "suspending"
					|| claimed.point != marker.point
					|| claimed.generation != marker.generation
					|| claimed.owner != lease.owner_node
					|| claimed.epoch != lease.epoch
				{
					return Err(EngineError::busy("suspending marker changed while being recovered"));
				}
				self
					.inner
					.net_runtime
					.block_on(handoff.release_source_volume_leases(&marker.sid))?;
				handoff.commit_suspend(&marker.sid, &lease.owner_node, lease.epoch)?;
				let committed = SuspensionMarker {
					owner: lease.owner_node.clone(),
					epoch: lease.epoch,
					state: "suspended".to_owned(),
					..marker.clone()
				};
				self.mesh_install_suspended_placeholder(&claimed_identity.record, &committed)
			})();
			if let Err(error) = result {
				tracing::debug!(sandbox = %marker.sid, %error, "portable suspending marker reconciliation deferred");
			}
		}
		Ok(())
	}

	fn reconcile_lifecycle_transition(
		&self,
		record: VmRecord,
		lifecycle: LifecycleState,
		pid_alive: bool,
	) -> Result<()> {
		let capture_lock = self.capture_lock(&record.id);
		let _capture_guard = capture_lock.acquire();
		if lifecycle.desired == LifecyclePhase::Running
			&& matches!(lifecycle.operation, Some(LifecycleOperation::Rollback { .. }))
		{
			let Some(LifecycleOperation::Rollback { recovery_point }) = lifecycle.operation.as_ref()
			else {
				unreachable!("rollback operation was matched above");
			};
			let portable = {
				let mut ownership = self.inner.portable_ownership.lock();
				if ownership.is_none() {
					*ownership = PortableOwnership::connect(&self.inner.config)?;
				}
				ownership.as_ref().cloned()
			};
			if !self.rollback_journal_path(&record.id)?.is_file()
				&& let Some(portable) = portable
				&& let Some(marker) = portable.rollback_marker(&record.id)?
			{
				if marker.operation_generation != lifecycle.generation.0 {
					let error =
						EngineError::engine("rollback marker does not match local lifecycle generation");
					self.fail_state_transition(&record.id, lifecycle.generation, &error);
					return Err(error);
				}
				let handoff = self
					.inner
					.restore_handoff
					.lock()
					.as_ref()
					.and_then(Weak::upgrade)
					.ok_or_else(|| EngineError::engine("portable rollback lacks ownership handoff"))?;
				let lease = portable.claim_expected(
					&record.id,
					&marker.owner,
					marker.epoch,
					marker.operation_generation,
				)?;
				if let Err(error) = handoff.begin_restore(&record.id, &lease.owner_node, lease.epoch) {
					let _ = portable.abort(&lease);
					return Err(error);
				}
				let heartbeat = match portable.start_restore_heartbeat(&lease) {
					Ok(heartbeat) => heartbeat,
					Err(error) => {
						let _ = handoff.abort_restore(&record.id, &lease.owner_node, lease.epoch);
						let _ = portable.abort(&lease);
						return Err(error);
					},
				};
				self
					.inner
					.launch_cancellations
					.lock()
					.insert(record.id.clone(), heartbeat.lost_signal());
				let restored = self.restore_recovery_identity(
					&record.id,
					&marker.safety,
					Some((Arc::clone(&handoff), lease.owner_node.clone(), lease.epoch)),
				);
				self.inner.launch_cancellations.lock().remove(&record.id);
				let authorization_error = match self.inner.net_runtime.block_on(heartbeat.finish()) {
					Ok(true) => None,
					Ok(false) => Some(EngineError::busy("portable rollback lease was lost")),
					Err(error) => Some(error),
				};
				if let Err(error) = restored {
					if let Some(candidate) = self.inner.registry.get(&record.id) {
						let _ = self.teardown(&candidate);
						self.inner.registry.remove(&record.id);
					}
					let _ = handoff.abort_restore(&record.id, &lease.owner_node, lease.epoch);
					let _ = portable.abort(&lease);
					self.fail_state_transition(&record.id, lifecycle.generation, &error);
					return Err(error);
				}
				if let Some(error) = authorization_error {
					if let Some(candidate) = self.inner.registry.get(&record.id) {
						let _ = self.teardown(&candidate);
						self.inner.registry.remove(&record.id);
					}
					let _ = handoff.abort_restore(&record.id, &lease.owner_node, lease.epoch);
					let _ = portable.abort(&lease);
					self.fail_state_transition(&record.id, lifecycle.generation, &error);
					return Err(error);
				}
				if let Err(error) = handoff.commit_rollback(&record.id, &lease.owner_node, lease.epoch)
					&& handoff
						.commit_rollback(&record.id, &lease.owner_node, lease.epoch)
						.is_err()
				{
					// Commit outcome is ambiguous. Keep the replacement,
					// fresh votes, and exact rolling-back marker for
					// idempotent fenced convergence.
					return Err(error);
				}
				self.disarm_restore_volume_leases(&record.id);
				self.complete_state_transition(
					&record.id,
					lifecycle.generation,
					LifecyclePhase::Running,
				)?;
				return Ok(());
			}

			// Once rollback created a durable journal, recovery must use the
			// journal's safety point rather than blindly retrying a target that
			// may already have destroyed the source. This same idempotent path
			// is used at daemon startup and by live maintenance reconciliation.
			if self.rollback_journal_path(&record.id)?.is_file() {
				// A durable journal is written only after the target pin succeeds.
				// Do not infer or reclaim another node's fencing identity here.
				return self.recover_rollback_journals();
			}
			// A journal is the destructive rollback authorization. A crash
			// before one exists must never retry the target: the original VM
			// may only have been paused while a safety point was being made.
			if pid_alive {
				control_for_vm(&self.sandbox(&record.name))?.resume()?;
				self.inner.registry.cancel_transition(
					self.home(),
					&record.id,
					lifecycle.generation,
					LifecyclePhase::Running,
					"rollback interrupted before safety recovery was durable",
				)?;
				let owner = record.detail.get("rollback_owner").and_then(Value::as_str);
				let epoch = record
					.detail
					.get("rollback_owner_epoch")
					.and_then(Value::as_i64);
				if let (Some(history), Some(owner), Some(epoch)) =
					(&self.inner.portable_history, owner, epoch)
				{
					history.release_rollback_target(
						&record.id,
						recovery_point,
						lifecycle.generation.0,
						owner,
						epoch,
					)?;
				}
				self.clear_rollback_detail(&record.id);
			} else {
				let error = EngineError::engine(
					"rollback interrupted before safety recovery; source availability is unknown",
				);
				self.fail_state_transition(&record.id, lifecycle.generation, &error);
			}
			return Ok(());
		}

		if lifecycle.desired == LifecyclePhase::Suspended {
			let portable = {
				let mut ownership = self.inner.portable_ownership.lock();
				if ownership.is_none() {
					*ownership = PortableOwnership::connect(&self.inner.config)?;
				}
				ownership.as_ref().cloned()
			};
			if let Some(portable) = portable
				&& let Some(marker) = portable.suspend_marker(&record.id)?
			{
				if marker.generation != lifecycle.generation.0
					|| !matches!(marker.state.as_str(), "suspending" | "suspended")
				{
					let error = EngineError::engine(
						"suspend reconciliation marker does not match local lifecycle generation",
					);
					self.fail_state_transition(&record.id, lifecycle.generation, &error);
					return Err(error);
				}
				let local_point = record
					.detail
					.get("suspend_recovery_point")
					.and_then(Value::as_str)
					.filter(|point| !point.is_empty());
				if marker.state == "suspending" && local_point.is_none() {
					if !pid_alive {
						// The atomically published marker is authoritative:
						// the source is gone, so complete its local
						// non-serving projection from this exact point.
						self.mesh_update_detail_fields(
							&record.id,
							Map::from_iter([("suspend_recovery_point".to_owned(), json!(marker.point))]),
						)?;
						self.persist_status(&record.id, "suspended", None, None)?;
						let handoff = self
							.inner
							.restore_handoff
							.lock()
							.as_ref()
							.and_then(Weak::upgrade)
							.ok_or_else(|| {
								EngineError::engine("portable suspend lacks ownership handoff")
							})?;
						self
							.inner
							.net_runtime
							.block_on(handoff.release_source_volume_leases(&record.id))?;
						handoff.commit_suspend(&record.id, &marker.owner, marker.epoch)?;
						self.complete_state_transition(
							&record.id,
							lifecycle.generation,
							LifecyclePhase::Suspended,
						)?;
						return Ok(());
					}
					let handoff = self
						.inner
						.restore_handoff
						.lock()
						.as_ref()
						.and_then(Weak::upgrade)
						.ok_or_else(|| EngineError::engine("portable suspend lacks ownership handoff"))?;
					let owner = (marker.owner.clone(), marker.epoch);
					self.abort_suspend_then_resume(&record.id, Some(&handoff), Some(&owner))?;
					self.inner.registry.cancel_transition(
						self.home(),
						&record.id,
						lifecycle.generation,
						LifecyclePhase::Running,
						"suspend publication was interrupted before local point persistence",
					)?;
					return Ok(());
				}
				if let Some(point) = local_point
					&& point != marker.point
				{
					let error =
						EngineError::engine("suspend reconciliation point does not match durable marker");
					self.fail_state_transition(&record.id, lifecycle.generation, &error);
					return Err(error);
				}
				if marker.state == "suspending" {
					if pid_alive {
						let returncode = self.teardown(&record)?;
						self.persist_status(&record.id, "suspended", returncode, None)?;
					} else {
						self.persist_status(&record.id, "suspended", None, None)?;
					}
					let handoff = self
						.inner
						.restore_handoff
						.lock()
						.as_ref()
						.and_then(Weak::upgrade)
						.ok_or_else(|| EngineError::engine("portable suspend lacks ownership handoff"))?;
					self
						.inner
						.net_runtime
						.block_on(handoff.release_source_volume_leases(&record.id))?;
					handoff.commit_suspend(&record.id, &marker.owner, marker.epoch)?;
					self.complete_state_transition(
						&record.id,
						lifecycle.generation,
						LifecyclePhase::Suspended,
					)?;
					return Ok(());
				}
				// PG already committed suspension: only reconstruct the
				// local non-serving placeholder, never infer a point.
				self.mesh_update_detail_fields(
					&record.id,
					Map::from_iter([("suspend_recovery_point".to_owned(), json!(marker.point))]),
				)?;
				self.persist_status(&record.id, "suspended", None, None)?;
				self.complete_state_transition(
					&record.id,
					lifecycle.generation,
					LifecyclePhase::Suspended,
				)?;
				return Ok(());
			}
		}

		// A crash before a recovery point is committed is an aborted suspend,
		// never a durable suspended state. Unpause and durably cancel the
		// intent so reconciliation does not retry a stale generation forever.
		if lifecycle.desired == LifecyclePhase::Suspended
			&& pid_alive
			&& record
				.detail
				.get("suspend_recovery_point")
				.and_then(Value::as_str)
				.is_none()
		{
			// Resume first: a crash after this point leaves the pending suspend
			// replayable, never a paused VM recorded as steady Running.
			control_for_vm(&self.sandbox(&record.name))?.resume()?;
			self.inner.registry.cancel_transition(
				self.home(),
				&record.id,
				lifecycle.generation,
				LifecyclePhase::Running,
				"suspend publication was interrupted before its recovery point committed",
			)?;
			return Ok(());
		}
		if lifecycle.desired == LifecyclePhase::Running
			&& pid_alive
			&& self.converge_pending_portable_resume(&record)?
		{
			return Ok(());
		}
		let transition = self.inner.registry.begin_transition(
			self.home(),
			&record.id,
			lifecycle.generation,
			lifecycle.desired.clone(),
		)?;
		let transition = match transition.disposition {
			TransitionDisposition::Acquired => transition,
			TransitionDisposition::Joined => {
				self
					.inner
					.registry
					.resume_transition(self.home(), &record.id, lifecycle.generation)?
			},
			TransitionDisposition::AlreadyObserved => return Ok(()),
		};
		if transition.disposition != TransitionDisposition::Acquired {
			return Ok(());
		}
		let generation = transition.generation;
		let vm = self.sandbox(&record.name);
		let outcome = match lifecycle.desired {
			LifecyclePhase::Paused if pid_alive => {
				control_for_vm(&vm)?.pause()?;
				Ok(LifecyclePhase::Paused)
			},
			LifecyclePhase::Running if pid_alive => {
				let _ = control_for_vm(&vm)?.resume()?;
				Ok(LifecyclePhase::Running)
			},
			LifecyclePhase::Stopped if pid_alive => {
				let code = self.teardown(&record)?;
				self.persist_status(&record.id, "stopped", code, None)?;
				Ok(LifecyclePhase::Stopped)
			},
			LifecyclePhase::Suspended if pid_alive => {
				if record
					.detail
					.get("suspend_recovery_point")
					.and_then(Value::as_str)
					.is_none()
				{
					// An interrupted pre-publication suspend must not strand a
					// live VMM paused. Resume it and retain the failed desired
					// state so a later retry gets a fresh generation.
					control_for_vm(&vm)?.resume()?;
					Err(EngineError::engine(
						"suspend publication was interrupted before its recovery point committed",
					))
				} else {
					let code = self.teardown(&record)?;
					self.persist_status(&record.id, "suspended", code, None)?;
					Ok(LifecyclePhase::Suspended)
				}
			},
			LifecyclePhase::Suspended if !pid_alive => record
				.detail
				.get("suspend_recovery_point")
				.and_then(Value::as_str)
				.filter(|name| !name.is_empty())
				.map(|_| LifecyclePhase::Suspended)
				.ok_or_else(|| {
					EngineError::engine(
						"cannot acknowledge suspended sandbox without its committed recovery point",
					)
				}),
			phase if !pid_alive && phase == LifecyclePhase::Stopped => Ok(LifecyclePhase::Stopped),
			phase => Err(EngineError::engine(format!(
				"cannot converge desired lifecycle state {} without a live sandbox",
				phase.as_str()
			))),
		};
		match outcome {
			Ok(observed) => self
				.inner
				.registry
				.observe_transition(self.home(), &record.id, generation, observed)
				.map(|_| ()),
			Err(error) => {
				self.fail_state_transition(&record.id, generation, &error);
				Err(error)
			},
		}
	}

	fn maintain_sandbox(&self, record: &VmRecord) -> Result<()> {
		if !record.lifecycle.is_converged() {
			return Ok(());
		}
		if record.detail.get("observed_state").and_then(Value::as_str) == Some("paused") {
			return Ok(());
		}
		let active = self.sample_guest_activity(&record.id, &record.name)?;
		let current = self
			.inner
			.registry
			.get(&record.id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{}'", record.id)))?;
		if !current.lifecycle.is_converged() || current.status != "running" {
			return Ok(());
		}
		let idle =
			idle_deadline_elapsed(&current, self.inner.config.idle_timeout, active, unix_time());
		if idle {
			<Self as EngineApi>::suspend(self, &record.id)?;
			self.inc_counter("idle_reaped");
			return Ok(());
		}
		let now = u64::try_from(unix_millis()).unwrap_or(u64::MAX);
		for (kind, cadence, field) in [
			("disk", self.inner.config.history_disk_sec, "last_disk_history_unix_millis"),
			(
				"checkpoint",
				self.inner.config.history_checkpoint_sec,
				"last_checkpoint_history_unix_millis",
			),
		] {
			if cadence <= 0.0 {
				continue;
			}
			let last = current
				.detail
				.get(field)
				.and_then(Value::as_u64)
				.unwrap_or_else(|| (current.created_at * 1000.0).max(0.0) as u64);
			if now.saturating_sub(last) < (cadence * 1000.0) as u64 {
				continue;
			}
			self.capture_recovery(&record.id, kind, true)?;
			self.mesh_update_detail_fields(
				&record.id,
				Map::from_iter([(field.to_owned(), json!(now))]),
			)?;
		}
		Ok(())
	}

	fn sample_guest_activity(&self, id: &str, name: &str) -> Result<Option<bool>> {
		let agent = self
			.inner
			.runtimes
			.lock()
			.get(name)
			.and_then(|runtime| runtime.agent.clone());
		let Some(agent) = agent else {
			return Ok(None);
		};
		let sample = agent.activity(Duration::from_secs(2))?;
		let (changed, network_delta) = {
			let mut runtimes = self.inner.runtimes.lock();
			let Some(runtime) = runtimes.get_mut(name) else {
				return Ok(None);
			};
			let previous = runtime.guest_activity.as_ref();
			let changed = previous.is_some_and(|previous| {
				sample.cpu_ticks.saturating_sub(previous.cpu_ticks) >= 10
					|| sample.disk_sectors != previous.disk_sectors
					|| sample.network_bytes != previous.network_bytes
			});
			let network_delta =
				previous.map(|previous| sample.network_bytes.saturating_sub(previous.network_bytes));
			runtime.guest_activity = Some(sample);
			(changed, network_delta)
		};
		if changed {
			let now = unix_time();
			self
				.inner
				.registry
				.update(id, |record| record.last_active = now);
			let _ = self
				.inner
				.registry
				.update_detail_persisted(self.home(), id, |detail| {
					detail.insert("last_active".to_owned(), json!(now));
				});
		}
		if let Some(network_delta) = network_delta {
			let threshold = self
				.inner
				.registry
				.get(id)
				.and_then(|record| {
					record
						.detail
						.get("activity_threshold_bytes")
						.and_then(Value::as_u64)
				})
				.unwrap_or(0);
			if network_delta_exceeds_threshold(network_delta, threshold) {
				let now = unix_time();
				self
					.inner
					.registry
					.update(id, |record| record.last_network_active = now);
				let _ = self
					.inner
					.registry
					.update_detail_persisted(self.home(), id, |detail| {
						detail.insert("last_network_active".to_owned(), json!(now));
					});
			}
		}
		Ok(Some(changed))
	}

	fn home(&self) -> &Home {
		&self.inner.home
	}

	fn sandbox(&self, name: &str) -> SandboxVm {
		self.inner.sandbox_runtime.sandbox(name)
	}

	fn disarm_restore_volume_leases(&self, id: &str) {
		if let Some(runtime) = self.inner.runtimes.lock().get_mut(id)
			&& let Some(leases) = runtime.restore_volume_leases.as_mut()
		{
			leases.disarm();
		}
	}

	/// Remove a not-yet-committed restore candidate before releasing its
	/// distributed writable-volume votes. A teardown error is fail-closed.
	fn remove_restore_candidate(&self, id: &str) -> Result<()> {
		if let Some(record) = self.inner.registry.get(id) {
			self.teardown(&record)?;
			self.inner.registry.remove(id);
		}
		Ok(())
	}

	/// Finish an already-materialized portable resume after its commit response
	/// was lost. This consumes only the exact locally-owned `resuming` epoch.
	fn converge_pending_portable_resume(&self, record: &VmRecord) -> Result<bool> {
		if record.status != "running"
			|| record.lifecycle.desired != LifecyclePhase::Running
			|| record.lifecycle.observed == LifecyclePhase::Running
		{
			return Ok(false);
		}
		let portable = {
			let mut ownership = self.inner.portable_ownership.lock();
			if ownership.is_none() {
				*ownership = PortableOwnership::connect(&self.inner.config)?;
			}
			ownership.as_ref().cloned()
		};
		let Some(portable) = portable else {
			return Ok(false);
		};
		let Some(marker) = portable.suspend_marker(&record.id)? else {
			return Ok(false);
		};
		if marker.state != "resuming"
			|| record.detail.get("recovery_point").and_then(Value::as_str)
				!= Some(marker.point.as_str())
		{
			return Ok(false);
		}
		let handoff = self
			.inner
			.restore_handoff
			.lock()
			.as_ref()
			.and_then(Weak::upgrade)
			.ok_or_else(|| EngineError::busy("resuming marker lacks a local ownership handoff"))?;
		let lease = portable.lease_for_resuming_marker(&marker)?.lease;
		if let Err(error) = handoff.commit_restore(&record.id, &lease.owner_node, lease.epoch)
			&& handoff
				.commit_restore(&record.id, &lease.owner_node, lease.epoch)
				.is_err()
		{
			return Err(error);
		}
		self.disarm_restore_volume_leases(&record.id);
		self.complete_state_transition(
			&record.id,
			record.lifecycle.generation,
			LifecyclePhase::Running,
		)?;
		Ok(true)
	}

	fn launch_sandbox(&self, vm: &SandboxVm, spec: &LaunchSpec) -> Result<()> {
		// Every launch funnels through this method (including restores and
		// rollback replacements), so production candidates always receive the
		// VMM-side ownership watchdog. Single-node launches deliberately do
		// not arm it.
		let spec = launch_spec_for_cluster(self.inner.config.cluster_mode, spec);
		self.inner.sandbox_runtime.launch(vm, &spec)?;
		vm.save_meta(Map::from_iter([(
			"runtime".to_owned(),
			json!(self.inner.sandbox_runtime.name()),
		)]))
	}

	fn stop_sandbox(&self, vm: &SandboxVm, wait: bool) -> Result<()> {
		self.inner.sandbox_runtime.stop(vm, wait)
	}

	fn remove_sandbox(&self, vm: &SandboxVm) -> Result<()> {
		self.inner.sandbox_runtime.remove(vm)
	}

	/// Remove only this node's candidate.  Mesh migration/fencing uses this
	/// path because the authoritative ownership/replica rows must remain
	/// intact until its own protocol commits their deletion.
	pub(crate) fn remove_local_candidate(&self, id: &str) -> Result<()> {
		if let Some(record) = self.inner.registry.get(id) {
			self.teardown(&record)?;
		}
		self.remove_sandbox(&self.sandbox(id))?;
		self.inner.registry.remove(id);
		Ok(())
	}

	/// Delete committed portable history only while the caller still holds the
	/// exact durable deletion tombstone.  The history implementation makes a
	/// completed retry a no-op after the ownership row has been finalized.
	pub(crate) fn delete_portable_history(&self, sid: &str, owner: &str, epoch: i64) -> Result<()> {
		if let Some(history) = &self.inner.portable_history {
			history.delete_sandbox_history(sid, owner, epoch)?;
		}
		Ok(())
	}

	fn delete_local_recovery_history(&self, sid: &str) -> Result<()> {
		match fs::remove_dir_all(self.recovery_root(sid)?) {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error.into()),
		}
	}

	/// History deletion is part of the portable deletion transaction: committing
	/// its tombstone first would make failed history cleanup unrecoverable.
	fn commit_portable_delete_after_history(
		delete_history: impl FnOnce() -> Result<()>,
		commit_delete: impl FnOnce() -> Result<()>,
	) -> Result<()> {
		delete_history()?;
		commit_delete()
	}

	fn sandbox_is_running(&self, vm: &SandboxVm) -> Result<bool> {
		self.inner.sandbox_runtime.is_running(vm)
	}

	fn rollback_uncommitted_runtime(
		&self,
		vm: &SandboxVm,
		runtime: &mut RuntimeState,
		retain_vm_dir: bool,
	) {
		if let Some(stop) = runtime.timeout_stop.take() {
			let _ = stop.send(());
		}
		if let Some(agent) = runtime.agent.take() {
			agent.close();
		}
		let _ = self.stop_sandbox(vm, true);
		for volume in &mut runtime.encrypted_volumes {
			let _ = volume.seal(&self.inner.keyring);
		}
		drop(runtime.credential_gateway.take());
		drop(runtime.s3_proxy.take());
		if let Some(network) = runtime.network.take() {
			let _ = network.teardown();
		} else {
			teardown_network(vm.name());
		}
		let _ = self.inner.vpcs.release_sandbox(vm.name());
		if !retain_vm_dir {
			let _ = self.remove_sandbox(vm);
		}
	}

	/// Once production has atomically published a `suspending` marker, the
	/// paused source may resume only after the exact marker abort succeeds.
	/// Otherwise a newer owner could be serving while this stale VM resumes.
	fn abort_suspend_then_resume(
		&self,
		id: &str,
		handoff: Option<&Arc<dyn OwnershipHandoff>>,
		owner: Option<&(String, i64)>,
	) -> Result<()> {
		let vm = self.sandbox(id);
		if !self.sandbox_is_running(&vm)? {
			return Ok(());
		}
		if let (Some(handoff), Some((owner, epoch))) = (handoff, owner)
			&& handoff.abort_suspend(id, owner, *epoch).is_err()
		{
			// PostgreSQL can apply the first idempotent abort before its
			// response is lost. Retry the exact fence before treating the
			// source as unsafe to resume.
			handoff.abort_suspend(id, owner, *epoch)?;
		}
		control_for_vm(&vm).and_then(|mut control| control.resume())?;
		Ok(())
	}

	fn reacquire_volume_locks(&self, requests: Vec<(String, Vec<String>)>) -> Result<()> {
		let mut requested = requests.into_iter().collect::<HashMap<_, _>>();
		let running = self
			.inner
			.registry
			.list()
			.into_iter()
			.filter(|record| record.status == "running")
			.collect::<Vec<_>>();
		let mut staged_by_name = HashMap::new();
		for record in &running {
			let mut staged = load_staged_volume_mounts(self.home(), &record.name)?;
			requested.entry(record.name.clone()).or_default().extend(
				staged
					.iter()
					.filter(|mount| !mount.read_only)
					.map(|mount| mount.name.clone()),
			);
			for mount in &mut staged {
				mount.arm();
			}
			staged_by_name.insert(record.name.clone(), staged);
		}
		let mut runtimes = self.inner.runtimes.lock();
		for record in running {
			let state = runtimes.entry(record.name.clone()).or_default();
			let mut names = requested.remove(&record.name).unwrap_or_default();
			names.sort();
			names.dedup();
			for volume_name in names {
				let volume = Volume::new_in_home(self.home().root(), &volume_name)?;
				state.volume_locks.push(volume.acquire_write_lock()?);
			}
			state.encrypted_volumes = staged_by_name.remove(&record.name).unwrap_or_default();
		}
		Ok(())
	}

	/// Rebuild only the durable, non-secret portion of a running sandbox's
	/// runtime state. Agent connections and credential material deliberately
	/// remain absent: they are re-established on first use.
	fn rehydrate_runtime_identities(&self) -> Result<()> {
		let records = self
			.inner
			.registry
			.list()
			.into_iter()
			.filter(|record| record.status == "running")
			.collect::<Vec<_>>();
		for record in records {
			let mut state = runtime_state_from_safe_identity(&record.runtime_identity);
			if state
				.network_spec
				.as_ref()
				.and_then(|spec| spec.get("flavor"))
				.and_then(Value::as_str)
				== Some("tap")
			{
				let ports = state
					.network_spec
					.as_ref()
					.and_then(|spec| spec.get("ports"))
					.and_then(Value::as_array)
					.into_iter()
					.flatten()
					.filter_map(Value::as_u64)
					.filter_map(|port| u16::try_from(port).ok())
					.collect::<Vec<_>>();
				let vpc_id = state
					.network_spec
					.as_ref()
					.and_then(|spec| spec.get("vpc"))
					.and_then(Value::as_str)
					.map(str::to_owned);
				state.network = Some(if let Some(vpc_id) = vpc_id {
					let guest_ip = state
						.network_spec
						.as_ref()
						.and_then(|spec| spec.get("guest_config"))
						.and_then(|config| config.get("guest_ip"))
						.and_then(Value::as_str)
						.map(str::to_owned)
						.ok_or_else(|| EngineError::engine("persisted VPC network has no guest IP"))?;
					let tenant = record
						.detail
						.get("owner_tenant")
						.and_then(Value::as_str)
						.unwrap_or("default");
					self
						.inner
						.vpcs
						.allocate(tenant, &vpc_id, &record.name, Some(&guest_ip))?;
					let (gateway, prefix) = self.inner.vpcs.gateway_and_prefix(tenant, &vpc_id)?;
					self.inner.net_runtime.block_on(net::setup_vpc_network(
						&record.name,
						&vpc_id,
						&guest_ip,
						&gateway,
						prefix,
						&ports,
						state.network_policy.inbound_cidr_allowlist.as_deref(),
					))?
				} else {
					self.inner.net_runtime.block_on(net::setup_sandbox_network(
						&record.name,
						&ports,
						state.network_policy.egress_allow.as_deref(),
						state.network_policy.egress_allow_domains.as_deref(),
						state.network_policy.inbound_cidr_allowlist.as_deref(),
					))?
				});
			}
			let services: Result<()> = (|| {
				let requested = record
					.detail
					.get("s3_mounts")
					.cloned()
					.map(serde_json::from_value::<HashMap<String, S3MountSpec>>)
					.transpose()?
					.unwrap_or_default();
				if !requested.is_empty() {
					let mut used_tags = HashSet::new();
					let mounts = self.resolve_s3_mounts(Some(requested), &mut used_tags)?;
					let routes = mounts
						.iter()
						.map(|mount| (mount.tag.clone(), Arc::clone(&mount.client)))
						.collect();
					state.s3_proxy = Some(S3Proxy::start(
						&self.inner.net_runtime,
						&self.sandbox(&record.name).dir().join("s3.sock"),
						routes,
					)?);
				}
				let names = record
					.detail
					.get("credential_names")
					.and_then(Value::as_array)
					.into_iter()
					.flatten()
					.filter_map(Value::as_str)
					.map(ToOwned::to_owned)
					.collect::<Vec<_>>();
				if !names.is_empty() {
					let network = state.network.as_ref().ok_or_else(|| {
						EngineError::engine(
							"credential gateway restart requires a reconstructed TAP network",
						)
					})?;
					let host_ip = network.config.host_ip.parse().map_err(|error| {
						EngineError::engine(format!(
							"invalid persisted credential-gateway host IP: {error}"
						))
					})?;
					let guest_ip = network.config.guest_ip.parse().map_err(|error| {
						EngineError::engine(format!(
							"invalid persisted credential-gateway guest IP: {error}"
						))
					})?;
					let tenant = record
						.detail
						.get("owner_tenant")
						.and_then(Value::as_str)
						.unwrap_or("default");
					self.start_credential_gateway_for(
						tenant, &record.id, &names, &mut state, host_ip, guest_ip,
					)?;
				}
				Ok(())
			})();
			if let Err(error) = services {
				let _ = self.teardown(&record);
				self.persist_status(&record.id, "stopped", None, None)?;
				self
					.inner
					.registry
					.update_detail_persisted(self.home(), &record.id, |detail| {
						detail.insert("restart_non_runnable".to_owned(), json!(error.to_string()));
					})?;
				continue;
			}
			let mut runtimes = self.inner.runtimes.lock();
			let existing = runtimes.entry(record.name).or_default();
			existing.env = state.env;
			existing.workdir = state.workdir;
			existing.network_policy = state.network_policy;
			existing.network_spec = state.network_spec;
			existing.network = state.network;
			existing.s3_proxy = state.s3_proxy;
			existing.credential_gateway = state.credential_gateway;
			existing.identity_complete = state.identity_complete;
		}
		Ok(())
	}

	fn recover_orphaned_encrypted_volumes(&self) -> Result<()> {
		let root = encrypted_volume_runtime_root(self.home());
		let entries = match fs::read_dir(&root) {
			Ok(entries) => entries,
			Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
			Err(error) => return Err(error.into()),
		};
		let active = self
			.inner
			.registry
			.list()
			.into_iter()
			.filter(|record| record.status == "running")
			.map(|record| record.name)
			.collect::<HashSet<_>>();
		for entry in entries {
			let entry = entry?;
			let sid = entry.file_name().into_string().map_err(|_| {
				EngineError::invalid("encrypted volume runtime contains a non-UTF-8 sandbox ID")
			})?;
			if active.contains(&sid) {
				continue;
			}
			let mut mounts = load_staged_volume_mounts(self.home(), &sid)?;
			for mount in &mut mounts {
				mount.arm();
			}
			for mount in &mut mounts {
				mount.seal(&self.inner.keyring)?;
			}
			drop(mounts);
			let sid_root = root.join(sid);
			if sid_root.exists() {
				fs::remove_dir_all(sid_root)?;
			}
		}
		Ok(())
	}

	fn start_configured_pools(&self) -> Result<()> {
		for warm in &self.inner.config.warm_images {
			self.start_warm_image(warm)?;
		}
		Ok(())
	}

	fn start_warm_image(&self, warm: &WarmImage) -> Result<()> {
		let request = TemplateRequest {
			image: Some(warm.reference.clone()),
			// Pools serve pool-eligible sandboxes (block_network, no volumes,
			// no fs_dir), whose templates carry no NIC slot.
			nic_slot: false,
			..TemplateRequest::default()
		};
		let cached = image::cached_template(self, &request)?;
		let key = template_key_for_cached(&cached);
		let pool = WarmPool::with_runtime(
			cached.snapshot_dir,
			warm.count,
			Arc::clone(&self.inner.sandbox_runtime),
		)?;
		let old = self.inner.pools.set(key, pool);
		if let Some(old) = old {
			old.shutdown();
		}
		Ok(())
	}

	fn validate_create(params: &SandboxCreate) -> Result<()> {
		require_positive("cpus", u64::from(params.cpus))?;
		require_positive("memory", u64::from(params.memory))?;
		require_positive("disk_mb", u64::from(params.disk_mb))?;
		if let Some(timeout) = params.timeout
			&& (!timeout.is_finite() || timeout < 0.0)
		{
			return Err(EngineError::invalid("timeout must be non-negative"));
		}
		if let Some(timeout) = params.idle_timeout_secs
			&& (!timeout.is_finite() || timeout < 0.0)
		{
			return Err(EngineError::invalid("idle_timeout_secs must be non-negative"));
		}
		if params
			.persistence
			.as_ref()
			.and_then(PersistencePolicy::sticky_priority)
			.is_some_and(|priority| priority > 10)
		{
			return Err(EngineError::invalid("sticky persistence priority must be between 0 and 10"));
		}
		if params.block_network && params.ports.as_ref().is_some_and(|ports| !ports.is_empty()) {
			return Err(EngineError::invalid("ports cannot be exposed when block_network=True"));
		}
		validate_ports(params.ports.as_deref())?;
		validate_cidrs("egress_allow", params.egress_allow.as_deref())?;
		validate_cidrs("inbound_cidr_allowlist", params.inbound_cidr_allowlist.as_deref())?;
		validate_domains("egress_allow_domains", params.egress_allow_domains.as_deref())?;
		validate_ha(params.ha.as_deref())?;
		if params.remote_page_url.is_some()
			|| params.remote_page_token.is_some()
			|| params.remote_page_digest.is_some()
		{
			return Err(EngineError::invalid("remote_page_* fields are server-internal"));
		}
		validate_arch(params.arch.as_deref())?;
		Ok(())
	}

	fn resolve_ha_encryption_key(&self, params: &mut SandboxCreate, ha: &str) -> Result<()> {
		if let Some(key_id) = Self::resolve_ha_key_id(
			&self.inner.config,
			ha,
			&params.owner_tenant,
			&params.encryption_key_id,
		)? {
			params.encryption_key_id = key_id;
		}
		Ok(())
	}

	fn resolve_ha_key_id(
		config: &ServeConfig,
		ha: &str,
		owner_tenant: &str,
		requested_key: &str,
	) -> Result<Option<String>> {
		if config.cluster_mode != ClusterMode::Production || ha == "off" {
			return Ok(None);
		}
		let shared_key = config
			.portable_history_key_id
			.as_deref()
			.filter(|key_id| !key_id.is_empty() && *key_id != "default");
		let tenant_key = config
			.tenant_keys
			.get(owner_tenant)
			.map(String::as_str)
			.filter(|key_id| !key_id.is_empty() && *key_id != "default");
		if !requested_key.is_empty() && requested_key != "default" {
			if shared_key == Some(requested_key) || tenant_key == Some(requested_key) {
				return Ok(Some(requested_key.to_owned()));
			}
			return Err(EngineError::invalid(
				"production HA encryption_key_id must be the shared key or the owner's configured \
				 tenant key",
			));
		}
		tenant_key
			.or(shared_key)
			.map(str::to_owned)
			.map(Some)
			.ok_or_else(|| {
				EngineError::invalid(
					"production HA sandboxes require a non-default shared or tenant encryption key",
				)
			})
	}

	fn prepare_create(&self, mut params: SandboxCreate) -> Result<CreatePlan> {
		params
			.idle_timeout_secs
			.get_or_insert(self.inner.config.idle_timeout);
		Self::validate_create(&params)?;
		let sid = params
			.name
			.clone()
			.filter(|name| !name.is_empty())
			.unwrap_or_else(|| format!("sb-{}", random_hex(12)));
		params.name = Some(sid.clone());
		validate_local_name("sandbox name", &sid)?;
		if self.inner.registry.get(&sid).is_some() || self.sandbox(&sid).dir().exists() {
			return Err(EngineError::busy(format!("sandbox '{sid}' already exists")));
		}
		let ha = match params.ha.as_deref().filter(|ha| !ha.is_empty()) {
			Some(ha) => ha.to_owned(),
			None => self.inner.config.default_ha(false).to_owned(),
		};
		let restart_policy = restart_policy_for_ha(&ha).to_owned();
		self.resolve_ha_encryption_key(&mut params, &ha)?;
		let relaunch_params = params.clone();
		let tags = params.tags.clone().unwrap_or_default();
		let secrets = parse_secrets(params.secrets.take())?;
		let secret_env = merge_secret_env(&secrets);
		let credential_names = params.credentials.clone().unwrap_or_default();
		if credential_names.iter().collect::<HashSet<_>>().len() != credential_names.len() {
			return Err(EngineError::invalid("credential names must be unique"));
		}
		for name in &credential_names {
			let credential = self.inner.credentials.get(&params.owner_tenant, name)?;
			if !credential.active_at(u64::try_from(unix_millis()).unwrap_or(u64::MAX)) {
				return Err(EngineError::invalid(format!("credential {name:?} has expired")));
			}
		}
		let timeout_secs = effective_timeout_secs(params.timeout_secs, params.timeout)?;
		let mut used_tags = HashSet::new();
		let mut volume_specs = self.resolve_volumes(
			&sid,
			&params.encryption_key_id,
			params.volumes.take(),
			&mut used_tags,
		)?;
		let mut s3_specs = self.resolve_s3_mounts(params.s3_mounts.take(), &mut used_tags)?;
		if let Some(tags) = params.s3_restore_tags.take() {
			apply_restored_s3_tags(&mut s3_specs, &volume_specs, &tags)?;
		}
		let n_vols = u64::try_from(volume_specs.len()).unwrap_or(WARM_VOLUME_SLOTS + 1);
		let warm_volumes = params.block_network
			&& params.fs_dir.is_none()
			&& params.template.is_none()
			&& s3_specs.is_empty()
			&& (1..=WARM_VOLUME_SLOTS).contains(&n_vols);
		let host_slot = params.block_network
			&& params.fs_dir.is_some()
			&& params.template.is_none()
			&& s3_specs.is_empty()
			&& volume_specs.is_empty();
		let networked_warm = !params.block_network
			&& cfg!(target_os = "macos")
			&& params.template.is_none()
			&& params.fs_dir.is_none()
			&& s3_specs.is_empty()
			&& volume_specs.is_empty()
			&& params.ports.as_ref().is_none_or(Vec::is_empty)
			&& params.egress_allow.as_ref().is_none_or(Vec::is_empty)
			&& params
				.egress_allow_domains
				.as_ref()
				.is_none_or(Vec::is_empty)
			&& params
				.inbound_cidr_allowlist
				.as_ref()
				.is_none_or(Vec::is_empty);
		let networked_warm_linux = !params.block_network
			&& !cfg!(target_os = "macos")
			&& params.template.is_none()
			&& params.fs_dir.is_none()
			&& s3_specs.is_empty()
			&& volume_specs.is_empty();
		let fs_slots = if warm_volumes { n_vols } else { 0 };
		let (template_dir, image_spec, image_ref, cached_key) = self.resolve_template(
			&params,
			fs_slots,
			host_slot,
			networked_warm,
			networked_warm_linux,
		)?;
		if warm_volumes {
			for (index, spec) in volume_specs.iter_mut().enumerate() {
				spec.tag = image::slot_tag(index as u64);
			}
		}
		Ok(CreatePlan {
			params,
			sid,
			ha,
			restart_policy,
			tags,
			secrets,
			secret_env,
			timeout_secs,
			volume_specs,
			template_dir,
			image_spec,
			image_ref,
			pool_key: cached_key,
			warm_volumes,
			host_slot,
			networked_warm,
			networked_warm_linux,
			s3_specs,
			relaunch_params,
			retained_rootfs: false,
		})
	}

	fn prepare_relaunch(&self, recipe: &RelaunchRecipe) -> Result<CreatePlan> {
		let mut params = recipe.params.clone();
		let sid = params
			.name
			.clone()
			.filter(|name| !name.is_empty())
			.ok_or_else(|| EngineError::engine("relaunch recipe has no sandbox identity"))?;
		let relaunch_params = params.clone();
		let ha = params
			.ha
			.clone()
			.unwrap_or_else(|| self.inner.config.default_ha(false).to_owned());
		let restart_policy = restart_policy_for_ha(&ha).to_owned();
		let tags = params.tags.clone().unwrap_or_default();
		let secrets = parse_secrets(params.secrets.take())?;
		let secret_env = merge_secret_env(&secrets);
		let timeout_secs = effective_timeout_secs(params.timeout_secs, params.timeout)?;
		let mut used_tags = HashSet::new();
		let volume_specs = self.resolve_volumes(
			&sid,
			&params.encryption_key_id,
			params.volumes.take(),
			&mut used_tags,
		)?;
		let s3_specs = self.resolve_s3_mounts(params.s3_mounts.take(), &mut used_tags)?;
		Ok(CreatePlan {
			params,
			sid,
			ha,
			restart_policy,
			tags,
			secrets,
			secret_env,
			timeout_secs,
			volume_specs,
			s3_specs,
			template_dir: recipe.template_dir.clone(),
			image_spec: recipe.image_spec.clone(),
			image_ref: recipe.image_ref.clone(),
			pool_key: String::new(),
			warm_volumes: false,
			host_slot: false,
			networked_warm: false,
			networked_warm_linux: false,
			relaunch_params,
			retained_rootfs: true,
		})
	}

	fn resolve_template(
		&self,
		params: &SandboxCreate,
		fs_slots: u64,
		host_slot: bool,
		nic_slot: bool,
		tap_slot: bool,
	) -> Result<(PathBuf, Option<image::ImageConfig>, Option<String>, String)> {
		if let Some(template) = &params.template {
			let path = PathBuf::from(template);
			let template_dir = if path.exists() || path.is_absolute() {
				path
			} else {
				self.home().templates_dir().join(template)
			};
			return Ok((template_dir, None, Some(template.clone()), template.clone()));
		}
		let memo_key = params.dockerfile.is_none().then(|| TemplateMemoKey {
			image: params.image.clone(),
			disk_mb: u64::from(params.disk_mb),
			memory: u64::from(params.memory),
			cpus: u64::from(params.cpus),
			fs_slots,
			host_slot,
			nic_slot,
			tap_slot,
		});
		if let Some(key) = &memo_key
			&& let Some(cached) = self.lookup_template_memo(key)
		{
			let pool_key = template_key_for_cached(&cached);
			return Ok((cached.snapshot_dir, Some(cached.spec), Some(cached.name), pool_key));
		}
		let request = TemplateRequest {
			image: params.image.clone(),
			dockerfile: params.dockerfile.as_ref().map(PathBuf::from),
			context: PathBuf::from(&params.context),
			disk_mb: u64::from(params.disk_mb),
			timeout: params.timeout.unwrap_or(300.0).max(0.0) as u64,
			memory: u64::from(params.memory),
			cpus: u64::from(params.cpus),
			fs_slots,
			host_slot,
			nic_slot,
			tap_slot,
		};
		let cached = image::cached_template(self, &request)?;
		if let Some(key) = memo_key {
			self.inner.template_memo.lock().insert(key, cached.clone());
		}
		let key = template_key_for_cached(&cached);
		Ok((cached.snapshot_dir, Some(cached.spec), Some(cached.name), key))
	}

	/// Return a memoized template only while its on-disk snapshot is still
	/// materialized (two stats); a template invalidated behind our back drops
	/// out of the memo and takes the full resolution path again.
	fn lookup_template_memo(&self, key: &TemplateMemoKey) -> Option<CachedTemplate> {
		let mut memo = self.inner.template_memo.lock();
		let cached = memo.get(key)?;
		if snapshot_state_present(&cached.snapshot_dir)
			&& cached.snapshot_dir.join("rootfs.img").is_file()
		{
			return Some(cached.clone());
		}
		memo.remove(key);
		None
	}

	fn resolve_volumes(
		&self,
		sid: &str,
		key_id: &str,
		volumes: Option<HashMap<String, Value>>,
		used_tags: &mut HashSet<String>,
	) -> Result<Vec<ResolvedVolume>> {
		let mut out = Vec::new();
		for (mountpoint, value) in volumes.unwrap_or_default() {
			let (name, read_only) = parse_volume_spec(&value)?;
			let volume = Volume::new_in_home(self.home().root(), &name)?;
			let lock = if read_only {
				None
			} else {
				Some(volume.acquire_write_lock()?)
			};
			let tag = unique_volume_tag(&name, used_tags);
			let encrypted = self.materialize_encrypted_volume(sid, &volume, key_id, read_only)?;
			out.push(ResolvedVolume {
				mountpoint,
				name,
				tag,
				host_dir: encrypted.mount_dir.clone(),
				read_only,
				lock,
				encrypted: Some(encrypted),
			});
		}
		Ok(out)
	}

	fn materialize_encrypted_volume(
		&self,
		sid: &str,
		volume: &Volume,
		key_id: &str,
		read_only: bool,
	) -> Result<EncryptedVolumeMount> {
		drop(self.inner.keyring.load(key_id)?);
		let archive = encrypted_volume_archive(self.home(), volume.name());
		let runtime_root = encrypted_volume_runtime_root(self.home()).join(sid);
		create_private_dir(&runtime_root)?;
		let slot_dir = runtime_root.join(volume.name());
		if slot_dir.exists() || slot_dir.is_symlink() {
			return Err(EngineError::busy(format!(
				"encrypted volume staging path {} already exists",
				slot_dir.display()
			)));
		}
		let result = (|| {
			let mount_dir = if archive.exists() || archive.is_symlink() {
				let metadata = fs::symlink_metadata(&archive)?;
				if metadata.file_type().is_symlink() || !metadata.is_file() {
					return Err(EngineError::invalid(format!(
						"encrypted volume archive {} is not a regular file",
						archive.display()
					)));
				}
				let mount_dir = EncryptedArchive::open(&archive, &slot_dir, &self.inner.keyring)?;
				if volume_has_plaintext(volume.path().as_path())? {
					remove_legacy_volume_data(volume.path().as_path())?;
				}
				mount_dir
			} else {
				let mount_dir = slot_dir.join(volume.name());
				create_private_dir(&mount_dir)?;
				if volume_has_plaintext(volume.path().as_path())? {
					copy_tree_without_locks(&volume.path(), &mount_dir)?;
				}
				EncryptedArchive::seal(&mount_dir, &archive, &self.inner.keyring, key_id)?;
				remove_legacy_volume_data(volume.path().as_path())?;
				mount_dir
			};
			write_volume_staging_manifest(&slot_dir, volume.name(), key_id, read_only)?;
			Ok(EncryptedVolumeMount {
				name: volume.name().to_owned(),
				mount_dir,
				slot_dir: slot_dir.clone(),
				archive,
				key_id: key_id.to_owned(),
				read_only,
				sealed: read_only,
				preserve: false,
			})
		})();
		if result.is_err() {
			let _ = fs::remove_dir_all(&slot_dir);
		}
		result
	}

	fn resolve_s3_mounts(
		&self,
		mounts: Option<HashMap<String, S3MountSpec>>,
		used_tags: &mut HashSet<String>,
	) -> Result<Vec<ResolvedS3Mount>> {
		let mounts = mounts.unwrap_or_default();
		if mounts.len() > MAX_S3_MOUNTS {
			return Err(EngineError::invalid(format!(
				"at most {MAX_S3_MOUNTS} S3 mounts are supported"
			)));
		}
		let mut mounts = mounts.into_iter().collect::<Vec<_>>();
		mounts.sort_unstable_by(|(left, _), (right, _)| left.cmp(right));
		let mut out = Vec::with_capacity(mounts.len());
		for (mountpoint, spec) in mounts {
			if !Path::new(&mountpoint).is_absolute() {
				return Err(EngineError::invalid(format!(
					"s3 mountpoint must be absolute: {mountpoint}"
				)));
			}
			let (bucket, prefix) = parse_s3_uri(&spec.uri)?;
			let region = spec
				.region
				.clone()
				.filter(|region| !region.is_empty())
				.unwrap_or_else(|| {
					std::env::var("AWS_REGION")
						.ok()
						.filter(|region| !region.is_empty())
						.unwrap_or_else(|| "us-east-1".to_owned())
				});
			let (creds, auth) = s3_credentials(&mountpoint, &spec)?;
			let client = Arc::new(
				S3Client::new(S3MountConfig {
					bucket: bucket.clone(),
					prefix: prefix.clone(),
					region: region.clone(),
					endpoint: spec.endpoint.clone(),
					read_only: spec.read_only,
					creds,
					auth,
				})
				.map_err(|error| EngineError::invalid(format!("s3 mount '{mountpoint}': {error}")))?,
			);
			self
				.inner
				.net_runtime
				.block_on(client.probe())
				.map_err(|error| EngineError::invalid(format!("s3 mount '{mountpoint}': {error}")))?;
			let tag = unique_volume_tag(&bucket, used_tags);
			out.push(ResolvedS3Mount {
				mountpoint,
				tag: tag.clone(),
				read_only: spec.read_only,
				client,
				meta: json!({
					"uri": spec.uri,
					"endpoint": spec.endpoint,
					"region": region,
					"read_only": spec.read_only,
					"tag": tag,
					"auth": auth.as_str(),
				}),
			});
		}
		Ok(out)
	}

	/// Rebuild remote filesystem clients from a snapshot's credential-free mount
	/// metadata.
	fn snapshot_s3_mounts(
		&self,
		snapshot_dir: &Path,
		requested: Option<HashMap<String, S3MountSpec>>,
	) -> Result<Vec<ResolvedS3Mount>> {
		let path = snapshot_dir.join(S3_MOUNTS_FILE);
		if !path.is_file() {
			if requested.as_ref().is_some_and(|mounts| !mounts.is_empty()) {
				return Err(EngineError::invalid(
					"cannot add S3 mounts to a snapshot without remote filesystem devices",
				));
			}
			return Ok(Vec::new());
		}
		let source = serde_json::from_slice::<Value>(&read_snapshot_metadata(&path)?)?;
		if source.as_object().is_some_and(Map::is_empty) {
			if requested.as_ref().is_some_and(|mounts| !mounts.is_empty()) {
				return Err(EngineError::invalid(
					"cannot add S3 mounts to a snapshot without remote filesystem devices",
				));
			}
			return Ok(Vec::new());
		}
		let mut params = Map::from_iter([("s3_mounts".to_owned(), source)]);
		let tags = restore_s3_mount_params(&mut params)?.ok_or_else(|| {
			EngineError::invalid(format!(
				"snapshot {} has invalid S3 mount metadata",
				snapshot_dir.display()
			))
		})?;
		let mounts = match requested {
			Some(mounts) => mounts,
			None => serde_json::from_value(
				params
					.remove("s3_mounts")
					.expect("restored S3 mount parameters remain present"),
			)?,
		};
		let mut used_tags = HashSet::new();
		let mut mounts = self.resolve_s3_mounts(Some(mounts), &mut used_tags)?;
		apply_restored_s3_tags(&mut mounts, &[], &tags)?;
		Ok(mounts)
	}

	/// Start a per-VM proxy before its remote virtio-fs devices connect.
	fn with_s3_proxy(
		&self,
		vm: &SandboxVm,
		mut spec: LaunchSpec,
		mounts: &[ResolvedS3Mount],
	) -> Result<(LaunchSpec, Option<S3Proxy>)> {
		if mounts.is_empty() {
			return Ok((spec, None));
		}
		let sock = vm.dir().join("s3.sock");
		let routes = mounts
			.iter()
			.map(|mount| (mount.tag.clone(), Arc::clone(&mount.client)))
			.collect();
		let proxy = S3Proxy::start(&self.inner.net_runtime, &sock, routes)?;
		for mount in mounts {
			spec = spec.with_remote_fs(RemoteFsShare { tag: mount.tag.clone(), sock: sock.clone() });
		}
		Ok((spec, Some(proxy)))
	}

	/// Mount each remote virtio-fs device into its guest target.
	fn mount_s3_in_guest(agent: &AgentConn, mounts: &[ResolvedS3Mount]) -> Result<()> {
		for mount in mounts {
			let mountpoint = Path::new(&mount.mountpoint);
			if mount.read_only {
				agent.mount(&mount.tag, mountpoint, true, "virtiofs", AGENT_REQUEST_TIMEOUT)?;
			} else {
				agent.mount_overlay(&mount.tag, mountpoint, AGENT_REQUEST_TIMEOUT)?;
			}
		}
		Ok(())
	}

	fn start_credential_gateway(
		&self,
		plan: &CreatePlan,
		runtime: &mut RuntimeState,
		bind_ip: IpAddr,
		guest_ip: IpAddr,
	) -> Result<()> {
		self.start_credential_gateway_for(
			&plan.params.owner_tenant,
			&plan.sid,
			plan.params.credentials.as_deref().unwrap_or(&[]),
			runtime,
			bind_ip,
			guest_ip,
		)
	}

	fn start_credential_gateway_for(
		&self,
		tenant: &str,
		sandbox_id: &str,
		names: &[String],
		runtime: &mut RuntimeState,
		bind_ip: IpAddr,
		guest_ip: IpAddr,
	) -> Result<()> {
		if names.is_empty() || runtime.credential_gateway.is_some() {
			return Ok(());
		}
		let provider: Arc<dyn CredentialProvider> = self.inner.credentials.clone();
		let gateway = CredentialGateway::start(
			&self.inner.net_runtime,
			bind_ip,
			guest_ip,
			if cfg!(target_os = "linux") {
				CREDENTIAL_GATEWAY_PORT
			} else {
				0
			},
			tenant.to_owned(),
			sandbox_id.to_owned(),
			names.to_vec(),
			provider,
			self.inner.audit.clone(),
		)?;
		if cfg!(target_os = "linux") {
			runtime
				.network
				.as_mut()
				.ok_or_else(|| EngineError::engine("credential gateway requires a TAP network"))?
				.allow_credential_gateway()?;
		}
		runtime
			.env
			.insert("VMON_CREDENTIAL_GATEWAY".to_owned(), gateway.endpoint().to_owned());
		runtime.credential_gateway = Some(gateway);
		Ok(())
	}

	/// Install a shared cancellation signal for a restore before any launch
	/// work starts. Lease-loss reconciliation sets this signal; every launch
	/// and readiness boundary observes the same atomic.
	pub(crate) fn begin_restore_cancellation(&self, id: &str) -> Arc<AtomicBool> {
		self
			.inner
			.launch_cancellations
			.lock()
			.entry(id.to_owned())
			.or_insert_with(|| Arc::new(AtomicBool::new(false)))
			.clone()
	}

	/// Remove the cancellation signal only after restore ownership has reached
	/// a terminal outcome and all candidate cleanup has completed.
	pub(crate) fn end_restore_cancellation(&self, id: &str) {
		self.inner.launch_cancellations.lock().remove(id);
	}

	fn launch_cancellation(&self, sid: &str) -> Option<Arc<AtomicBool>> {
		self.inner.launch_cancellations.lock().get(sid).cloned()
	}

	fn ensure_launch_not_cancelled(cancellation: Option<&AtomicBool>) -> Result<()> {
		if cancellation.is_some_and(|signal| signal.load(Ordering::Acquire)) {
			return Err(EngineError::busy("sandbox launch fenced before readiness"));
		}
		Ok(())
	}

	fn launch_create(
		&self,
		plan: &mut CreatePlan,
		start_paused: bool,
	) -> Result<(SandboxVm, RuntimeState)> {
		let cancellation = self.launch_cancellation(&plan.sid);
		Self::ensure_launch_not_cancelled(cancellation.as_deref())?;
		let mut runtime = RuntimeState {
			secret_env: plan.secret_env.clone(),
			env: image_env(plan.image_spec.as_ref(), plan.params.env.as_ref()),
			workdir: Some(
				plan
					.params
					.workdir
					.clone()
					.or_else(|| plan.image_spec.as_ref().map(|spec| spec.workdir.clone()))
					.unwrap_or_else(|| "/".to_owned()),
			),
			network_policy: NetworkPolicy {
				block_network:          Some(plan.params.block_network),
				egress_allow:           plan.params.egress_allow.clone(),
				egress_allow_domains:   plan.params.egress_allow_domains.clone(),
				inbound_cidr_allowlist: plan.params.inbound_cidr_allowlist.clone(),
			},
			identity_complete: true,
			..RuntimeState::default()
		};
		let vm = self.claim_or_launch_vm(plan, &mut runtime, start_paused)?;
		// A lease-loss callback can install its signal while the runtime is
		// creating VM state. Re-read after claim so that post-spawn window is
		// fenced before any agent/network/volume registration.
		let cancellation = self.launch_cancellation(&plan.sid).or(cancellation);
		if let Err(error) = Self::ensure_launch_not_cancelled(cancellation.as_deref()) {
			self.rollback_uncommitted_runtime(&vm, &mut runtime, plan.retained_rootfs);
			return Err(error);
		}
		// Heartbeats use canonical resource keys after a worker restart; VMM
		// metadata calls memory `mem`, and paused candidates return before setup.
		let resource_meta = Map::from_iter([
			("cpus".to_owned(), json!(plan.params.cpus)),
			("memory".to_owned(), json!(plan.params.memory)),
			("disk_mb".to_owned(), json!(plan.params.disk_mb)),
		]);
		if let Err(error) = vm.save_meta(resource_meta) {
			self.rollback_uncommitted_runtime(&vm, &mut runtime, plan.retained_rootfs);
			return Err(error);
		}
		runtime.volume_locks = plan
			.volume_specs
			.iter_mut()
			.filter_map(|volume| volume.lock.take())
			.collect();
		runtime.encrypted_volumes = plan
			.volume_specs
			.iter_mut()
			.filter_map(|volume| volume.encrypted.take())
			.collect();
		for volume in &mut runtime.encrypted_volumes {
			volume.arm();
		}
		if start_paused {
			// The VMM has acknowledged its control socket but has executed no
			// guest instructions. Keep the exact resolved plan in memory so
			// activation can perform the ordinary setup once, after ownership
			// becomes durable.
			runtime.pending_setup = Some(Box::new(std::mem::replace(plan, CreatePlan {
				params:               SandboxCreate::default(),
				sid:                  String::new(),
				ha:                   String::new(),
				restart_policy:       String::new(),
				tags:                 HashMap::new(),
				secrets:              Vec::new(),
				secret_env:           BTreeMap::new(),
				timeout_secs:         None,
				volume_specs:         Vec::new(),
				s3_specs:             Vec::new(),
				template_dir:         PathBuf::new(),
				image_spec:           None,
				image_ref:            None,
				pool_key:             String::new(),
				warm_volumes:         false,
				host_slot:            false,
				networked_warm:       false,
				networked_warm_linux: false,
				relaunch_params:      SandboxCreate::default(),
				retained_rootfs:      false,
			})));
			return Ok((vm, runtime));
		}
		let setup_result = (|| {
			let agent =
				Self::agent_for_vm_cancellable(&vm, AGENT_CONNECT_TIMEOUT, cancellation.as_deref())?;
			if let Some(timeout_secs) = plan.timeout_secs
				&& runtime.timeout_stop.is_none()
			{
				runtime.timeout_stop = Some(start_timeout_watchdog(
					vm.name().to_owned(),
					timeout_secs,
					Arc::clone(&self.inner.sandbox_runtime),
				));
			}
			if runtime.timeout_stop.is_some() && plan.pool_key.is_empty() {
				// no-op branch kept explicit: fresh launches pass --timeout-secs to
				// the VMM.
			}
			for volume in &plan.volume_specs {
				agent.mount(
					&volume.tag,
					Path::new(&volume.mountpoint),
					volume.read_only,
					"virtiofs",
					AGENT_REQUEST_TIMEOUT,
				)?;
			}
			Self::mount_s3_in_guest(&agent, &plan.s3_specs)?;
			if let Some(network) = &runtime.network {
				let gc = &network.guest_config;
				agent.net_config(
					&gc.guest_ip,
					gc.prefix,
					&gc.host_ip,
					Some(&gc.dns),
					AGENT_REQUEST_TIMEOUT,
				)?;
				runtime.network_spec = Some(json!({
					"flavor": "tap",
					"guest_config": network_guest_json(&network.guest_config),
					"ports": sorted_ports(plan.params.ports.as_deref(), &network.tunnels()),
					"tunnels": tunnels_json(&network.tunnels()),
					"policy": policy_json(&runtime.network_policy),
					"vpc": plan.params.nics.as_ref().and_then(|nics| nics.first()).map(|nic| &nic.vpc),
				}));
			} else if network_required(&plan.params) {
				let gc = user_net_guest_config();
				let dns = net::USER_NET_DNS
					.iter()
					.map(|dns| (*dns).to_owned())
					.collect::<Vec<_>>();
				agent.net_config(
					gc["guest_ip"].as_str().unwrap_or(net::USER_NET_GUEST_IP),
					gc["prefix"]
						.as_u64()
						.unwrap_or_else(|| u64::from(net::USER_NET_PREFIX)) as u8,
					gc["host_ip"].as_str().unwrap_or(net::USER_NET_GATEWAY),
					Some(&dns),
					AGENT_REQUEST_TIMEOUT,
				)?;
				runtime.network_spec = Some(json!({
					"flavor": "user",
					"guest_config": gc,
					"ports": [],
					"tunnels": {},
					"policy": policy_json(&runtime.network_policy),
				}));
			}
			if let Some(probe) = &plan.params.readiness_probe {
				Self::wait_until_ready(
					&agent,
					&runtime,
					probe,
					plan.params.timeout.unwrap_or(300.0),
					cancellation.as_deref(),
				)?;
			}
			runtime.agent = Some(agent);
			let mut meta = Map::new();
			meta.insert("sandbox".to_owned(), json!(true));
			meta.insert("image".to_owned(), json!(plan.image_ref));
			meta.insert("template".to_owned(), json!(plan.template_dir.to_string_lossy()));
			meta.insert("workdir".to_owned(), json!(runtime.workdir));
			meta.insert("env_names".to_owned(), json!(runtime.env.keys().collect::<Vec<_>>()));
			meta.insert(
				"secret_names".to_owned(),
				json!(
					plan
						.secrets
						.iter()
						.map(|secret| &secret.name)
						.collect::<Vec<_>>()
				),
			);
			meta.insert("credential_names".to_owned(), json!(plan.params.credentials));
			meta.insert("owner_tenant".to_owned(), json!(plan.params.owner_tenant));
			meta.insert("encryption_key_id".to_owned(), json!(plan.params.encryption_key_id));
			meta.insert("desired_state".to_owned(), json!("running"));
			meta.insert("observed_state".to_owned(), json!("running"));
			meta.insert("state_generation".to_owned(), json!(1));
			meta.insert("tags".to_owned(), json!(plan.tags));
			meta.insert("volumes".to_owned(), volumes_meta(&plan.volume_specs));
			meta.insert("s3_mounts".to_owned(), s3_mounts_meta(&plan.s3_specs));
			meta.insert("block_network".to_owned(), json!(plan.params.block_network));
			meta.insert("network".to_owned(), runtime.network_spec.clone().unwrap_or(Value::Null));
			meta.insert("timeout_secs".to_owned(), json!(plan.timeout_secs));
			if let Some(idle_timeout_secs) = plan.params.idle_timeout_secs {
				meta.insert("idle_timeout_secs".to_owned(), json!(idle_timeout_secs));
			}
			if let Some(activity_threshold_bytes) = plan.params.activity_threshold_bytes {
				meta.insert("activity_threshold_bytes".to_owned(), json!(activity_threshold_bytes));
			}
			meta.insert("runtime_identity".to_owned(), runtime_identity(&runtime));
			vm.save_meta(meta)?;
			Ok(())
		})();
		if let Err(error) = setup_result {
			self.rollback_uncommitted_runtime(&vm, &mut runtime, plan.retained_rootfs);
			return Err(error);
		}
		if let Err(error) = Self::ensure_launch_not_cancelled(cancellation.as_deref()) {
			self.rollback_uncommitted_runtime(&vm, &mut runtime, plan.retained_rootfs);
			return Err(error);
		}
		Ok((vm, runtime))
	}

	fn claim_or_launch_vm(
		&self,
		plan: &CreatePlan,
		runtime: &mut RuntimeState,
		start_paused: bool,
	) -> Result<SandboxVm> {
		if cfg!(target_os = "macos") && credentials_requested(&plan.params) {
			self.start_credential_gateway(
				plan,
				runtime,
				"127.0.0.1".parse().expect("loopback IP"),
				net::USER_NET_GATEWAY.parse().expect("user-net gateway IP"),
			)?;
		}
		if plan.retained_rootfs {
			return self.launch_cold_vm(plan, runtime, start_paused);
		}
		if (plan.params.block_network || plan.networked_warm)
			&& plan.volume_specs.is_empty()
			&& plan.s3_specs.is_empty()
			&& plan.params.fs_dir.is_none()
			&& !credentials_requested(&plan.params)
		{
			if plan.params.pool_size > 0 {
				let pool = WarmPool::with_runtime(
					&plan.template_dir,
					plan.params.pool_size as usize,
					Arc::clone(&self.inner.sandbox_runtime),
				)?;
				let old = self
					.inner
					.pools
					.set(plan.pool_key.clone(), Arc::clone(&pool));
				if let Some(old) = old {
					old.shutdown();
				}
			}
			if let Some(pool) = self.inner.pools.get(&plan.pool_key)
				&& let Some(vm) = pool.claim(Some(&plan.sid), start_paused)?
			{
				if plan.networked_warm {
					runtime.network_spec = Some(json!({
						"flavor": "user",
						"guest_config": user_net_guest_config(),
						"ports": [],
						"tunnels": {},
						"policy": {},
					}));
				}
				if let Some(secs) = plan.timeout_secs {
					let _ = control_for_vm(&vm)?.extend(secs);
					runtime.timeout_stop = Some(start_timeout_watchdog(
						vm.name().to_owned(),
						secs,
						Arc::clone(&self.inner.sandbox_runtime),
					));
				}
				return Ok(vm);
			}
		}
		if plan.params.block_network
			&& plan.params.fs_dir.is_none()
			&& plan.volume_specs.is_empty()
			&& plan.s3_specs.is_empty()
			&& !credentials_requested(&plan.params)
			&& snapshot_state_present(&plan.template_dir)
		{
			return self.launch_restore_vm(plan, None, runtime, start_paused);
		}
		if plan.warm_volumes || plan.host_slot || plan.networked_warm {
			return self.launch_restore_vm(plan, None, runtime, start_paused);
		}
		if (plan.networked_warm_linux
			|| (network_required(&plan.params)
				&& plan.s3_specs.is_empty()
				&& snapshot_state_present(&plan.template_dir)))
			&& !cfg!(target_os = "macos")
		{
			let network = self.setup_network(&plan.sid, &plan.params)?;
			let tap = network.guest_config.tap.clone();
			let host_ip = network
				.guest_config
				.host_ip
				.parse()
				.map_err(|_| EngineError::engine("allocated host gateway is not an IP address"))?;
			runtime.network = Some(network);
			self.start_credential_gateway(plan, runtime, host_ip, host_ip)?;
			return self.launch_restore_vm(plan, Some(tap), runtime, start_paused);
		}
		if network_required(&plan.params)
			&& plan.s3_specs.is_empty()
			&& cfg!(target_os = "macos")
			&& snapshot_state_present(&plan.template_dir)
		{
			reject_macos_host_network_features(&plan.params)?;
			return self.launch_restore_vm(plan, None, runtime, start_paused);
		}
		self.launch_cold_vm(plan, runtime, start_paused)
	}

	fn launch_restore_vm(
		&self,
		plan: &CreatePlan,
		tap: Option<String>,
		runtime: &mut RuntimeState,
		start_paused: bool,
	) -> Result<SandboxVm> {
		let vm = self.sandbox(&plan.sid);
		let mut spec = LaunchSpec::restore(vm.api_sock(), &plan.template_dir)
			.with_agent_sock(vm.dir().join("agent.sock"))
			.with_mem_mib(u64::from(plan.params.memory))
			.with_cpus(u64::from(plan.params.cpus));
		// The restored VM must own its disk: overlay the template's image so
		// guest writes land in a per-VM file (checkpointable, migratable)
		// instead of the shared absolute path recorded in the snapshot's
		// block-device hint — a path that does not even exist on a peer node
		// restoring a pulled checkpoint.
		let base_disk = plan.template_dir.join("rootfs.img");
		if base_disk.is_file() {
			spec = spec.with_disk_overlay(base_disk, vm.dir().join("rootfs.img"));
		}
		if let Some(secs) = plan.timeout_secs {
			spec = spec.with_timeout_secs(secs);
		}
		if let Some(tap) = tap {
			spec = spec.with_tap(tap);
		} else if network_required(&plan.params) {
			spec = spec.with_user_net();
		}
		if cfg!(target_os = "macos") && credentials_requested(&plan.params) {
			let port = runtime
				.credential_gateway
				.as_ref()
				.ok_or_else(|| EngineError::engine("credential gateway was not started"))?
				.port();
			spec = spec.with_restricted_user_net(port);
		}
		if let Some(fs_dir) = &plan.params.fs_dir {
			spec = spec.with_fs_share("host", fs_dir);
		}
		for volume in &plan.volume_specs {
			spec = spec.with_volume(VolumeMount::new(
				volume.tag.clone(),
				volume.host_dir.clone(),
				volume.read_only,
			)?);
		}
		if let Some(url) = &plan.params.remote_page_url {
			spec = spec.with_remote_page(
				url,
				plan.params.remote_page_token.clone(),
				plan.params.remote_page_digest.clone(),
			);
		}
		if start_paused {
			spec = spec.with_start_paused();
		}
		let (spec, s3_proxy) = self.with_s3_proxy(&vm, spec, &plan.s3_specs)?;
		self.launch_sandbox(&vm, &spec)?;
		copy_agent_marker(&plan.template_dir, vm.dir())?;
		runtime.s3_proxy = s3_proxy;
		if network_required(&plan.params) && runtime.network.is_none() {
			runtime.network_spec = Some(json!({
				"flavor": "user",
				"guest_config": user_net_guest_config(),
				"ports": [],
				"tunnels": {},
				"policy": {},
			}));
		}
		Ok(vm)
	}

	fn launch_cold_vm(
		&self,
		plan: &CreatePlan,
		runtime: &mut RuntimeState,
		start_paused: bool,
	) -> Result<SandboxVm> {
		let vm = self.sandbox(&plan.sid);
		let base_disk = plan.template_dir.join("rootfs.img");
		let rootfs = vm.dir().join("rootfs.img");
		if plan.retained_rootfs {
			if !rootfs.is_file() {
				return Err(EngineError::not_found(format!(
					"sandbox '{}' has no retained rootfs.img",
					plan.sid
				)));
			}
		} else if !base_disk.is_file() {
			return Err(EngineError::engine(format!(
				"template {} has no rootfs.img; fresh-boot sandboxes require a disk-backed template",
				plan.template_dir.display()
			)));
		}
		let kernel = image::assets::default_kernel()?;
		let mut spec = LaunchSpec::boot_rootfs(vm.api_sock(), kernel, &rootfs)
			.with_agent_sock(vm.dir().join("agent.sock"));
		if !plan.retained_rootfs {
			spec = spec.with_disk_overlay(base_disk, &rootfs);
		}
		spec = spec
			.with_mem_mib(u64::from(plan.params.memory))
			.with_cpus(u64::from(plan.params.cpus))
			.with_rng()
			.with_snapshot_root(self.snapshot_root());
		if let Some(secs) = plan.timeout_secs {
			spec = spec.with_timeout_secs(secs);
		}
		if network_required(&plan.params) {
			if cfg!(target_os = "macos") {
				reject_macos_host_network_features(&plan.params)?;
				spec = spec.with_user_net();
				if credentials_requested(&plan.params) {
					let port = runtime
						.credential_gateway
						.as_ref()
						.ok_or_else(|| EngineError::engine("credential gateway was not started"))?
						.port();
					spec = spec.with_restricted_user_net(port);
				}
			} else {
				let network = self.setup_network(&plan.sid, &plan.params)?;
				let tap = network.guest_config.tap.clone();
				let host_ip =
					network.guest_config.host_ip.parse().map_err(|_| {
						EngineError::engine("allocated host gateway is not an IP address")
					})?;
				runtime.network = Some(network);
				self.start_credential_gateway(plan, runtime, host_ip, host_ip)?;
				spec = spec.with_tap(tap);
			}
		}
		if let Some(fs_dir) = &plan.params.fs_dir {
			spec = spec.with_fs_share("host", fs_dir);
		}
		for volume in &plan.volume_specs {
			spec = spec.with_volume(VolumeMount::new(
				volume.tag.clone(),
				volume.host_dir.clone(),
				volume.read_only,
			)?);
		}
		if start_paused {
			spec = spec.with_start_paused();
		}
		let (spec, s3_proxy) = self.with_s3_proxy(&vm, spec, &plan.s3_specs)?;
		self.launch_sandbox(&vm, &spec)?;
		copy_agent_marker(&plan.template_dir, vm.dir())?;
		runtime.s3_proxy = s3_proxy;
		Ok(vm)
	}

	fn setup_network(&self, name: &str, params: &SandboxCreate) -> Result<SandboxNetwork> {
		if let Some(nics) = params.nics.as_deref()
			&& !nics.is_empty()
		{
			if nics.len() != 1 || !nics[0].default {
				return Err(EngineError::invalid("vmon VMs have a single NIC"));
			}
			let nic = &nics[0];
			let requested = match &nic.ipv4 {
				Value::Bool(true) => None,
				Value::String(address) => Some(address.as_str()),
				_ => {
					return Err(EngineError::invalid(
						"VPC NIC ipv4 must be a valid IPv4 address or true",
					));
				},
			};
			let guest_ip =
				self
					.inner
					.vpcs
					.allocate(&params.owner_tenant, &nic.vpc, name, requested)?;
			let (gateway, prefix) = self
				.inner
				.vpcs
				.gateway_and_prefix(&params.owner_tenant, &nic.vpc)?;
			let network = self.inner.net_runtime.block_on(net::setup_vpc_network(
				name,
				&nic.vpc,
				&guest_ip,
				&gateway,
				prefix,
				params.ports.as_deref().unwrap_or(&[]),
				params.inbound_cidr_allowlist.as_deref(),
			));
			if network.is_err() {
				let _ = self.inner.vpcs.release_sandbox(name);
			}
			return network;
		}
		let egress_allow = if params.block_network {
			Some(&[] as &[String])
		} else {
			params.egress_allow.as_deref()
		};
		let egress_allow_domains = if params.block_network {
			Some(&[] as &[String])
		} else {
			params.egress_allow_domains.as_deref()
		};
		self.inner.net_runtime.block_on(net::setup_sandbox_network(
			name,
			params.ports.as_deref().unwrap_or(&[]),
			egress_allow,
			egress_allow_domains,
			params.inbound_cidr_allowlist.as_deref(),
		))
	}

	fn wait_until_ready(
		agent: &AgentConn,
		runtime: &RuntimeState,
		probe: &Value,
		timeout: f64,
		cancellation: Option<&AtomicBool>,
	) -> Result<()> {
		let deadline = Instant::now() + Duration::from_secs_f64(timeout.max(0.0));
		let mut last = None;
		while Instant::now() < deadline {
			Self::ensure_launch_not_cancelled(cancellation)?;
			let remaining = deadline.saturating_duration_since(Instant::now());
			let attempt = remaining.min(Duration::from_secs(5));
			let result = if let Some(port) = probe.as_u64() {
				u16::try_from(port)
					.map_err(|_| EngineError::invalid("readiness port must be 1-65535"))
					.and_then(|port| agent.tcp_probe(port, "127.0.0.1", attempt))
					.and_then(|ready| {
						ready
							.then_some(())
							.ok_or_else(|| EngineError::engine("probe not ready"))
					})
			} else if let Some(port) = probe.get("port").and_then(Value::as_u64) {
				let host = probe
					.get("host")
					.and_then(Value::as_str)
					.unwrap_or("127.0.0.1");
				u16::try_from(port)
					.map_err(|_| EngineError::invalid("readiness port must be 1-65535"))
					.and_then(|port| agent.tcp_probe(port, host, attempt.min(Duration::from_secs(1))))
					.and_then(|ready| {
						ready
							.then_some(())
							.ok_or_else(|| EngineError::engine("probe not ready"))
					})
			} else {
				let argv = readiness_argv(probe);
				let env = merged_env(runtime, None);
				let cwd = runtime.workdir.as_deref().map(Path::new);
				agent
					.exec(&argv, cwd, Some(&env), false, Some(attempt))
					.and_then(|session| session.wait(Some(attempt)))
					.and_then(|code| {
						(code == 0)
							.then_some(())
							.ok_or_else(|| EngineError::engine("probe not ready"))
					})
			};
			match result {
				Ok(()) => return Ok(()),
				Err(err) => last = Some(err),
			}
			thread::sleep(
				Duration::from_millis(100).min(deadline.saturating_duration_since(Instant::now())),
			);
		}
		Err(EngineError::engine(format!(
			"sandbox readiness probe timed out after {timeout}s{}",
			last.map_or_else(String::new, |err| format!(": {err}"))
		)))
	}

	fn agent_for(&self, name: &str) -> Result<AgentConn> {
		let cached_agent = self
			.inner
			.runtimes
			.lock()
			.get(name)
			.and_then(|state| state.agent.clone());
		if let Some(agent) = cached_agent.filter(|agent| !agent.is_closed()) {
			return Ok(agent);
		}
		let vm = self.sandbox(name);
		let agent = Self::agent_for_vm(&vm, AGENT_CONNECT_TIMEOUT)?;
		self
			.inner
			.runtimes
			.lock()
			.entry(name.to_owned())
			.or_default()
			.agent = Some(agent.clone());
		Ok(agent)
	}

	fn agent_for_vm(vm: &SandboxVm, timeout: Duration) -> Result<AgentConn> {
		AgentConn::connect(&vm.agent_sock()?, timeout)
	}

	fn agent_for_vm_cancellable(
		vm: &SandboxVm,
		timeout: Duration,
		cancellation: Option<&AtomicBool>,
	) -> Result<AgentConn> {
		let deadline = Instant::now() + timeout;
		let mut last_error = None;
		while Instant::now() < deadline {
			Self::ensure_launch_not_cancelled(cancellation)?;
			let attempt = deadline
				.saturating_duration_since(Instant::now())
				.min(Duration::from_millis(100));
			match Self::agent_for_vm(vm, attempt) {
				Ok(agent) => return Ok(agent),
				Err(error) => last_error = Some(error),
			}
			Self::ensure_launch_not_cancelled(cancellation)?;
			thread::sleep(Duration::from_millis(10));
		}
		Self::ensure_launch_not_cancelled(cancellation)?;
		Err(last_error.unwrap_or_else(|| EngineError::engine("agent did not become reachable")))
	}

	fn start_entry_command(&self, name: String, cmd: Vec<String>) -> Result<()> {
		let agent = self.agent_for(&name)?;
		let (env, fallback_workdir) = {
			let runtimes = self.inner.runtimes.lock();
			runtimes.get(&name).map_or_else(
				|| (BTreeMap::new(), None),
				|state| (merged_env(state, None), state.workdir.clone()),
			)
		};
		let session =
			agent.exec(&cmd, fallback_workdir.as_deref().map(Path::new), Some(&env), false, None)?;
		let engine = self.clone();
		thread::Builder::new()
			.name(format!("vmon-entry-{name}"))
			.spawn(move || {
				if let Err(err) = engine.run_entry_command(name.clone(), session) {
					tracing::warn!(sandbox = %name, error = %err, "entry command failed");
				}
			})?;
		Ok(())
	}

	fn run_entry_command(
		&self,
		name: String,
		session: crate::engine::agent::ExecSession,
	) -> Result<()> {
		let log = Arc::new(Mutex::new(
			fs::OpenOptions::new()
				.create(true)
				.append(true)
				.open(self.sandbox(&name).log_path())?,
		));
		let parts = session.split();
		let _ = parts.control.close_stdin();
		let stdout = drain_entry_stream(parts.stdout, Arc::clone(&log));
		let stderr = drain_entry_stream(parts.stderr, Arc::clone(&log));
		match parts.exit.recv() {
			Ok(Ok(_status)) => {},
			Ok(Err(err)) => return Err(err),
			Err(_) => return Err(EngineError::engine("agent connection closed")),
		}
		let _ = stdout.join();
		let _ = stderr.join();
		Ok(())
	}

	fn get_record(&self, id: &str, require_running: bool) -> Result<VmRecord> {
		let mut record = self
			.inner
			.registry
			.get(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		// Always refresh: a VMM that died on its own (timeout self-kill, guest
		// poweroff) must be observable through plain GET/list, like Python's
		// inspect()/ps() liveness refresh.
		record = self.refresh_record_status(record)?;
		if require_running {
			if record.status != "running" {
				return Err(EngineError::not_running(format!("sandbox '{id}' is not running")));
			}
			// Only actions on a running sandbox count as activity; plain reads
			// must not advance `last_active` (it feeds `expires_at`).
			self.inner.registry.update(id, VmRecord::touch);
		}
		Ok(record)
	}

	fn refresh_record_status(&self, record: VmRecord) -> Result<VmRecord> {
		if record.status != "running" {
			// A stop that raced the VMM's own exit can persist "stopped" before
			// status.json lands; backfill the exit code once it is readable.
			if record.status == "stopped"
				&& record.detail.get("returncode").is_none_or(Value::is_null)
				&& let Some(returncode) = Self::poll_returncode(&record.name)
			{
				let _ = self.persist_status(&record.id, "stopped", Some(returncode), None);
				if let Some(updated) = self.inner.registry.get(&record.id) {
					return Ok(updated);
				}
			}
			return Ok(record);
		}
		let vm = self.sandbox(&record.name);
		if record.pid.is_some() && !self.sandbox_is_running(&vm)? {
			// Seal staged volumes and release runtime resources before publishing a
			// terminal state for autonomous guest exits.
			let returncode = Self::poll_returncode(&record.name);
			let sealed_returncode = self.teardown(&record)?.or(returncode);
			self.persist_status(&record.id, "stopped", sealed_returncode, None)?;
			let updated = self
				.inner
				.registry
				.get(&record.id)
				.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{}'", record.id)))?;
			self.publish_record_event("stopped", &updated);
			return Ok(updated);
		}
		Ok(record)
	}

	fn persist_status(
		&self,
		id: &str,
		status: &str,
		returncode: Option<i64>,
		terminated_at: Option<f64>,
	) -> Result<()> {
		self
			.inner
			.registry
			.persist_record_status(self.home(), id, status, returncode, terminated_at)
	}

	fn teardown(&self, record: &VmRecord) -> Result<Option<i64>> {
		let name = &record.name;
		let mut returncode = Self::poll_returncode(name);
		let vm = self.sandbox(name);
		if let Err(error) = self.stop_sandbox(&vm, true)
			&& self.sandbox_is_running(&vm).unwrap_or(true)
		{
			return Err(error);
		}
		if returncode.is_none() {
			returncode = Self::poll_returncode(name);
		}
		let removed = self.inner.runtimes.lock().remove(name);
		if let Some(mut state) = removed {
			if let Some(stop) = state.timeout_stop.take() {
				let _ = stop.send(());
			}
			if let Some(agent) = state.agent.take() {
				agent.close();
			}
			for volume in &mut state.encrypted_volumes {
				if let Err(error) = volume.seal(&self.inner.keyring) {
					self.inner.runtimes.lock().insert(name.to_owned(), state);
					return Err(error);
				}
			}
			drop(state.s3_proxy.take());
			if let Some(network) = state.network.take() {
				network.teardown()?;
			} else {
				teardown_network(name);
			}
			drop(state.volume_locks);
			drop(state.encrypted_volumes);
		} else {
			teardown_network(name);
		}
		Ok(returncode)
	}

	fn begin_state_transition(&self, id: &str, desired: LifecyclePhase) -> Result<TransitionBegin> {
		let record = self.get_record(id, false)?;
		self
			.inner
			.registry
			.begin_transition(self.home(), id, record.lifecycle.generation, desired)
	}

	fn complete_state_transition(
		&self,
		id: &str,
		generation: StateGeneration,
		observed: LifecyclePhase,
	) -> Result<()> {
		self
			.inner
			.registry
			.observe_transition(self.home(), id, generation, observed)?;
		self.wake_maintenance();
		Ok(())
	}

	fn fail_state_transition(&self, id: &str, generation: StateGeneration, error: &EngineError) {
		let _ = self
			.inner
			.registry
			.fail_transition(self.home(), id, generation, error.to_string());
	}

	fn poll_returncode(name: &str) -> Option<i64> {
		let vm = SandboxVm::new(name);
		// The jail-aware control-socket parent is a best-effort candidate; the
		// plain VM dir must still be probed when metadata is unreadable.
		let mut candidates = Vec::with_capacity(2);
		if let Some(parent) = vm
			.control_sock()
			.ok()
			.and_then(|sock| sock.parent().map(Path::to_path_buf))
		{
			candidates.push(parent.join("status.json"));
		}
		candidates.push(vm.dir().join("status.json"));
		for path in candidates {
			let Ok(text) = fs::read_to_string(path) else {
				continue;
			};
			let Ok(data) = serde_json::from_str::<Value>(&text) else {
				continue;
			};
			if let Some(code) = data.get("vmm_returncode").and_then(Value::as_i64) {
				return Some(code);
			}
			if let Some(reason) = data.get("reason").and_then(Value::as_str) {
				return match reason {
					"timeout" => Some(124),
					"quit" | "killed" => Some(137),
					"shutdown" => Some(0),
					_ => None,
				};
			}
		}
		None
	}

	fn snapshot_root(&self) -> PathBuf {
		self.home().root().join("snapshots")
	}

	fn snapshot_dir(&self, name: &str) -> PathBuf {
		self.snapshot_root().join(name)
	}

	fn snapshot_archive(&self, name: &str) -> Result<PathBuf> {
		validate_local_name("snapshot name", name)?;
		Ok(self.snapshot_root().join(format!("{name}.venc")))
	}

	fn open_snapshot(&self, name: &str) -> Result<SnapshotSource> {
		let archive = self.snapshot_archive(name)?;
		let metadata = match fs::symlink_metadata(&archive) {
			Ok(metadata) => metadata,
			Err(error) if error.kind() == io::ErrorKind::NotFound => {
				return Err(EngineError::not_found(format!("snapshot not found: {name}")));
			},
			Err(error) => return Err(error.into()),
		};
		if metadata.file_type().is_symlink() || !metadata.is_file() {
			return Err(EngineError::invalid(format!(
				"snapshot {name:?} is not a regular encrypted archive"
			)));
		}
		let mut sources = self.inner.snapshot_sources.lock();
		if let Some(source) = sources.get(name) {
			return Ok(source.clone());
		}
		let runtime_root = self.home().security_dir().join("runtime");
		fs::create_dir_all(&runtime_root)?;
		fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))?;
		let extract_root = runtime_root.join(format!("snapshot-{}", random_hex(24)));
		let path = EncryptedArchive::open(&archive, &extract_root, &self.inner.keyring)?;
		let source = SnapshotSource { path, guard: Some(Arc::new(TransientDir(extract_root))) };
		sources.insert(name.to_owned(), source.clone());
		Ok(source)
	}

	fn recovery_root(&self, id: &str) -> Result<PathBuf> {
		validate_local_name("sandbox name", id)?;
		Ok(self.home().security_dir().join("recovery").join(id))
	}

	fn recovery_archive(&self, id: &str, recovery_point: &str) -> Result<PathBuf> {
		validate_local_name("recovery point", recovery_point)?;
		Ok(self
			.recovery_root(id)?
			.join(format!("{recovery_point}.venc")))
	}

	fn recovery_points(&self, id: &str) -> Result<Vec<RecoveryPoint>> {
		if let Some(history) = &self.inner.portable_history {
			return history.history(id).map(|points| {
				points
					.into_iter()
					.filter(|point| !point.name.contains("-rollback-safety-"))
					.map(|point| RecoveryPoint {
						name:                   point.name,
						kind:                   point.kind,
						created_at_unix_millis: point.created_at_unix_millis,
						size_bytes:             point.size_bytes,
					})
					.collect()
			});
		}
		let root = self.recovery_root(id)?;
		let mut points = match fs::read_dir(root) {
			Ok(entries) => entries
				.filter_map(std::result::Result::ok)
				.filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
				.filter_map(|entry| {
					let file_name = entry.file_name();
					let name = file_name.to_str()?.strip_suffix(".venc")?;
					let mut fields = name.splitn(3, '-');
					let created_at_unix_millis = fields.next()?.parse().ok()?;
					let kind = fields.next()?.to_owned();
					let size_bytes = entry.metadata().ok()?.len();
					if name.contains("-rollback-safety-") {
						return None;
					}
					Some(RecoveryPoint {
						name: name.to_owned(),
						kind,
						created_at_unix_millis,
						size_bytes,
					})
				})
				.collect::<Vec<_>>(),
			Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
			Err(error) => return Err(error.into()),
		};
		points.sort_by_key(|point| point.created_at_unix_millis);
		Ok(points)
	}

	fn prune_recovery(&self, id: &str, protected: Option<&str>) -> Result<()> {
		let points = self.recovery_points(id)?;
		let retain = self.inner.config.history_retention.max(1);
		let now = u64::try_from(unix_millis()).unwrap_or(u64::MAX);
		let max_age_millis = (self.inner.config.history_max_age_sec * 1000.0) as u64;
		for (index, point) in points.iter().enumerate() {
			let over_count = points.len().saturating_sub(index) > retain;
			let expired =
				max_age_millis > 0 && now.saturating_sub(point.created_at_unix_millis) > max_age_millis;
			if point.name != protected.unwrap_or_default()
				&& (over_count || expired)
				&& index + 1 < points.len()
			{
				fs::remove_file(self.recovery_archive(id, &point.name)?)?;
			}
		}
		Ok(())
	}

	fn capture_recovery(&self, id: &str, kind: &str, keep_running: bool) -> Result<RecoveryPoint> {
		self.capture_recovery_with_portability(id, kind, keep_running, true)
	}

	fn capture_recovery_with_portability(
		&self,
		id: &str,
		kind: &str,
		keep_running: bool,
		publish_portable: bool,
	) -> Result<RecoveryPoint> {
		let lock = self.capture_lock(id);
		let _guard = lock.acquire();
		let record = self.get_record(id, false)?;
		if record.status != "running" || !record.lifecycle.is_converged() {
			return Err(EngineError::busy("sandbox lifecycle is not steady for recovery capture"));
		}
		self.capture_recovery_with_portability_unlocked(
			id,
			kind,
			keep_running,
			publish_portable,
			None,
		)
	}

	fn capture_recovery_with_portability_unlocked(
		&self,
		id: &str,
		kind: &str,
		keep_running: bool,
		publish_portable: bool,
		suspend_generation: Option<u64>,
	) -> Result<RecoveryPoint> {
		let recovery_kind = match kind {
			"disk" | "checkpoint" => kind,
			"rollback-safety" => "checkpoint",
			_ => return Err(EngineError::invalid(format!("unknown recovery kind {kind:?}"))),
		};
		#[cfg(test)]
		let capture_executor = self.inner.capture_executor.lock().clone();
		#[cfg(test)]
		if let Some(executor) = capture_executor {
			return executor(self, id, kind, keep_running, publish_portable);
		}
		let (record, mut params) = self.mesh_checkpoint_params(id)?;
		// This fences only the original live process. A safety/target
		// replacement must receive the configuration, never the token.
		params.remove("rollback_source_token");
		let created_at_unix_millis = u64::try_from(unix_millis()).unwrap_or(u64::MAX);
		let key_id = if publish_portable && self.inner.portable_history.is_some() {
			let record_key = record
				.detail
				.get("encryption_key_id")
				.and_then(Value::as_str)
				.filter(|key| *key != "default");
			let owner_tenant = record.detail.get("owner_tenant").and_then(Value::as_str);
			let tenant_key = owner_tenant
				.and_then(|tenant| self.inner.config.tenant_keys.get(tenant))
				.map(String::as_str);
			let key_id = record_key
				.filter(|key| tenant_key == Some(*key))
				.or(self.inner.config.portable_history_key_id.as_deref())
				.ok_or_else(|| {
					EngineError::invalid("production recovery requires portable_history_key_id")
				})?;
			// A mapped tenant key is portable only after PortableHistory setup
			// has verified its cluster-wide key fingerprint. An unmapped key
			// deliberately falls back to the shared recovery key.
			self.inner.keyring.load(key_id)?;
			key_id
		} else {
			record
				.detail
				.get("encryption_key_id")
				.and_then(Value::as_str)
				.unwrap_or("default")
		};
		params.remove("secrets");
		if let Some(credentials) = params.remove("credential_names") {
			params.insert("credentials".to_owned(), credentials);
		}
		params.insert("agent".to_owned(), json!(true));
		let runtime_root = self.home().security_dir().join("runtime");
		fs::create_dir_all(&runtime_root)?;
		fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))?;
		let capture_root = runtime_root.join(format!("capture-{}", random_hex(24)));
		let capture_guard = TransientDir(capture_root.clone());
		let vm = self.sandbox(&record.name);
		let mut resume_on_error =
			(recovery_kind == "checkpoint" && !keep_running && !publish_portable)
				.then(|| ResumeOnError::new(vm.clone()));
		let portable_owner = if publish_portable && self.inner.portable_history.is_some() {
			let mut ownership = self.inner.portable_ownership.lock();
			if ownership.is_none() {
				*ownership = PortableOwnership::connect(&self.inner.config)?;
			}
			Some(
				ownership
					.as_ref()
					.ok_or_else(|| {
						EngineError::engine(
							"production recovery publication requires ownership authority",
						)
					})?
					.current(id)?,
			)
		} else {
			None
		};
		let publication_generation = suspend_generation.unwrap_or(record.lifecycle.generation.0);
		let capture_generation = if let Some((owner, epoch)) = portable_owner.as_ref() {
			let ownership = self.inner.portable_ownership.lock();
			ownership
				.as_ref()
				.ok_or_else(|| EngineError::engine("production recovery ownership bridge disappeared"))?
				.allocate_checkpoint_generation(id, owner, *epoch)?
		} else {
			self.next_checkpoint_generation(id)?
		};
		let recovery_point = format!(
			"{created_at_unix_millis:020}-{kind}-{capture_generation:020}-{}{}",
			random_hex(8),
			if publish_portable {
				""
			} else {
				"-rollback-safety-"
			},
		);
		let archive = self.recovery_archive(id, &recovery_point)?;
		let _archive_cleanup = ArchiveCleanup {
			path:     archive.clone(),
			// Local recovery and the rollback safety journal deliberately own
			// their archive. Production uses it only as publish staging.
			preserve: !(publish_portable && self.inner.portable_history.is_some()),
		};
		// The VMM owns this transient disk-only artifact. Arm cleanup before
		// issuing control so a failed snapshot/copy never leaves an orphaned
		// plaintext rootfs under the shared snapshot root.
		let _disk_artifact_cleanup =
			(recovery_kind == "disk").then(|| TransientDir(self.snapshot_dir(&recovery_point)));
		if recovery_kind == "disk" {
			// The VMM quiesces only the writable block worker around the rootfs
			// clone. vCPUs and other devices remain live; disk points are
			// crash-consistent storage captures, not process checkpoints.
			let mut control = control_for_vm(&vm)?;
			let reply = control.disk_snapshot(&recovery_point)?;
			if reply.get("artifact").and_then(Value::as_str) != Some("disk")
				|| reply.get("rootfs").and_then(Value::as_str) != Some("rootfs.img")
			{
				return Err(EngineError::engine("VMM returned an invalid disk-only snapshot reply"));
			}
			let artifact = self.snapshot_dir(&recovery_point).join("disk");
			let rootfs = artifact.join("rootfs.img");
			let manifest = artifact.join("manifest.json");
			require_regular_file(&rootfs, "VMM disk rootfs")?;
			require_regular_file(&manifest, "VMM disk manifest")?;
			fs::create_dir_all(&capture_root)?;
			move_or_copy_regular_file(&rootfs, &capture_root.join("rootfs.img"))?;
			fs::copy(&manifest, capture_root.join("disk-manifest.json"))?;
			stamp_checkpoint_marker(&capture_root, record.detail.as_object())?;
			capture_checkpoint_volumes(
				self,
				&record.name,
				&capture_root,
				record.detail.get("volumes"),
			)?;
			ensure_checkpoint_template_present(&capture_root)?;
			let _ = fs::remove_dir_all(self.snapshot_dir(&recovery_point));
		} else {
			let disk = vm.rootfs_img().ok().filter(|path| path.is_file());
			let snapshot_name = format!("recovery-{}", random_hex(12));
			let snapshot_path = self.snapshot_dir(&snapshot_name);
			let _snapshot_cleanup = TransientDir(snapshot_path);
			let snapshot_path = self.snapshot_machine_while_paused(
				&vm,
				&snapshot_name,
				keep_running,
				disk.as_deref(),
				&self.snapshot_root(),
				false,
				!keep_running,
				|dir| {
					stamp_checkpoint_rootfs(self.home(), dir, record.detail.as_object())?;
					stamp_checkpoint_marker(dir, record.detail.as_object())?;
					capture_checkpoint_volumes(self, &record.name, dir, record.detail.get("volumes"))?;
					ensure_checkpoint_template_present(dir)
				},
			)?;
			fs::create_dir_all(&capture_root)?;
			fs::rename(snapshot_path, capture_root.join("state"))?;
		}
		fs::write(
			capture_root.join("recovery.json"),
			serde_json::to_vec(&json!({
				"version": 1,
				"sandbox_id": id,
				"kind": recovery_kind,
				"created_at_unix_millis": created_at_unix_millis,
				"params": params,
			}))?,
		)?;
		EncryptedArchive::seal(&capture_root, &archive, &self.inner.keyring, key_id)?;
		let mut size_bytes = fs::metadata(&archive)?.len();
		if publish_portable && let Some(history) = &self.inner.portable_history {
			let (owner_node, owner_epoch) = portable_owner.as_ref().ok_or_else(|| {
				EngineError::engine("portable publication ownership was not resolved")
			})?;
			let incarnation_epoch = self
				.inner
				.portable_ownership
				.lock()
				.as_ref()
				.ok_or_else(|| {
					EngineError::engine("portable publication ownership bridge disappeared")
				})?
				.current_incarnation(id, owner_node, *owner_epoch)?;
			let prepared = history.prepare(PortablePointInput {
				sid: id.to_owned(),
				name: recovery_point.clone(),
				kind: recovery_kind.to_owned(),
				created_at_unix_millis,
				archive: archive.clone(),
				owner_node: owner_node.clone(),
				owner_epoch: *owner_epoch,
				incarnation_epoch,
				lifecycle_generation: publication_generation,
			})?;
			let published = history.publish(&prepared)?;
			let committed = match suspend_generation {
				Some(lifecycle_generation) => history.commit_suspend(
					&published,
					RetentionPolicy::from_config(&self.inner.config),
					&PortableSuspendIntent {
						sid: id.to_owned(),
						owner: owner_node.clone(),
						epoch: *owner_epoch,
						point: recovery_point.clone(),
						lifecycle_generation,
					},
				),
				None => history.commit(&published, RetentionPolicy::from_config(&self.inner.config)),
			};
			let committed = match committed {
				Ok(committed) => committed,
				Err(error) => {
					let _ = history.abort(published);
					return Err(error);
				},
			};
			size_bytes = committed.size_bytes;
			// PostgreSQL now names the exact verified object. Local bytes are a
			// cache, not recovery authority in production.
			let _ = fs::remove_file(&archive);
		} else {
			self.prune_recovery(id, Some(&recovery_point))?;
		}
		if let Some(guard) = &mut resume_on_error {
			guard.disarm();
		}
		drop(capture_guard);
		Ok(RecoveryPoint {
			name: recovery_point,
			kind: recovery_kind.to_owned(),
			created_at_unix_millis,
			size_bytes,
		})
	}

	fn open_recovery(
		&self,
		id: &str,
		recovery_point: &str,
	) -> Result<(SnapshotSource, Map<String, Value>)> {
		let archive = self.recovery_archive(id, recovery_point)?;
		// Production recovery downloads are ephemeral staging bytes. The
		// committed recovery authority is PostgreSQL/S3, never this cache.
		let _download_cleanup = ArchiveCleanup {
			path:     archive.clone(),
			preserve: self.inner.portable_history.is_none(),
		};
		if let Some(history) = &self.inner.portable_history {
			let point = history.lookup(id, recovery_point)?.ok_or_else(|| {
				EngineError::not_found(format!("recovery point not found: {recovery_point}"))
			})?;
			history.download(&point, &archive)?;
		}
		let metadata = fs::symlink_metadata(&archive).map_err(|error| {
			if error.kind() == io::ErrorKind::NotFound {
				EngineError::not_found(format!("recovery point not found: {recovery_point}"))
			} else {
				error.into()
			}
		})?;
		if metadata.file_type().is_symlink() || !metadata.is_file() {
			return Err(EngineError::invalid(format!(
				"recovery point {recovery_point:?} is not a regular encrypted archive"
			)));
		}
		let runtime_root = self.home().security_dir().join("runtime");
		fs::create_dir_all(&runtime_root)?;
		fs::set_permissions(&runtime_root, fs::Permissions::from_mode(0o700))?;
		let extract_root = runtime_root.join(format!("recovery-{}", random_hex(24)));
		let path = EncryptedArchive::open(&archive, &extract_root, &self.inner.keyring)?;
		let manifest =
			serde_json::from_slice::<Value>(&read_snapshot_metadata(&path.join("recovery.json"))?)?;
		let kind = manifest
			.get("kind")
			.and_then(Value::as_str)
			.ok_or_else(|| EngineError::invalid("recovery manifest is missing its kind"))?;
		if manifest.get("version").and_then(Value::as_u64) != Some(1)
			|| manifest.get("sandbox_id").and_then(Value::as_str) != Some(id)
		{
			return Err(EngineError::invalid("recovery manifest does not match sandbox"));
		}
		let path = match kind {
			"checkpoint" => path.join("state"),
			"disk" => path,
			_ => return Err(EngineError::invalid("recovery manifest has an invalid kind")),
		};
		let params = manifest
			.get("params")
			.and_then(Value::as_object)
			.cloned()
			.ok_or_else(|| EngineError::invalid("recovery manifest is missing params"))?;
		Ok((SnapshotSource { path, guard: Some(Arc::new(TransientDir(extract_root))) }, params))
	}

	fn replacement_lifecycle(previous: &LifecycleState) -> LifecycleState {
		previous.clone()
	}

	fn rollback_journal_path(&self, id: &str) -> Result<PathBuf> {
		validate_local_name("sandbox name", id)?;
		Ok(self
			.home()
			.root()
			.join("rollback-journals")
			.join(format!("{id}.json")))
	}

	fn write_rollback_journal(&self, journal: &RollbackJournal) -> Result<()> {
		let path = self.rollback_journal_path(&journal.sandbox_id)?;
		let parent = path
			.parent()
			.ok_or_else(|| EngineError::engine("rollback journal lacks parent"))?;
		fs::create_dir_all(parent)?;
		let temporary = parent.join(format!(".{}.{}.tmp", journal.sandbox_id, random_hex(8)));
		let mut temp_guard = JournalTemp(temporary.clone());
		let bytes = serde_json::to_vec(journal)
			.map_err(|error| EngineError::engine(format!("serializing rollback journal: {error}")))?;
		let mut file = OpenOptions::new()
			.write(true)
			.create_new(true)
			.mode(0o600)
			.open(&temporary)?;
		file.write_all(&bytes)?;
		file.sync_all()?;
		fs::rename(&temporary, &path)?;
		temp_guard.disarm();
		OpenOptions::new().read(true).open(parent)?.sync_all()?;
		Ok(())
	}

	fn clear_rollback_journal(&self, id: &str) -> Result<()> {
		let path = self.rollback_journal_path(id)?;
		match fs::remove_file(&path) {
			Ok(()) => {
				let parent = path
					.parent()
					.ok_or_else(|| EngineError::engine("rollback journal lacks parent"))?;
				OpenOptions::new().read(true).open(parent)?.sync_all()?;
				Ok(())
			},
			Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error.into()),
		}
	}

	fn remove_safety_recovery(&self, id: &str, point: &str) -> Result<()> {
		match fs::remove_file(self.recovery_archive(id, point)?) {
			Ok(()) => Ok(()),
			Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
			Err(error) => Err(error.into()),
		}
	}

	fn clear_rollback_detail(&self, id: &str) {
		let _ = self
			.inner
			.registry
			.update_detail_persisted(self.home(), id, |detail| {
				detail.remove("lifecycle_operation");
				detail.remove("rollback_recovery_point");
				detail.remove("rollback_generation");
				detail.remove("rollback_source_token");
			});
	}

	fn release_rollback_pin(&self, journal: &RollbackJournal) -> Result<()> {
		if let Some(history) = &self.inner.portable_history {
			history.release_rollback_target(
				&journal.sandbox_id,
				&journal.target_recovery_point,
				journal.generation,
				&journal.portable_owner,
				journal.portable_owner_epoch,
			)?;
		}
		Ok(())
	}

	fn reconcile_rollback_pins(&self) -> Result<()> {
		if let Some(history) = &self.inner.portable_history {
			// A node cannot prove another owner's journal converged from its
			// local registry view. Pins are released only by that journal's
			// fenced success path; retention may safely clean up tombstones.
			let _ = history.list_rollback_pins()?;
		}
		Ok(())
	}

	fn finalize_rollback_journal(&self, journal: &RollbackJournal) -> Result<()> {
		self.remove_safety_recovery(&journal.sandbox_id, &journal.safety_recovery_point)?;
		self.release_rollback_pin(journal)?;
		self.clear_rollback_journal(&journal.sandbox_id)
	}

	fn recover_rollback_journals(&self) -> Result<()> {
		let directory = self.home().root().join("rollback-journals");
		let entries = match fs::read_dir(&directory) {
			Ok(entries) => entries,
			Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
			Err(error) => return Err(error.into()),
		};
		for entry in entries {
			let entry = entry?;
			if !entry.file_type()?.is_file() {
				continue;
			}
			let path = entry.path();
			let name = entry.file_name();
			let Some(name) = name.to_str() else {
				continue;
			};
			let Some(sandbox_id) = name.strip_suffix(".json") else {
				if name.starts_with('.')
					&& Path::new(name)
						.extension()
						.is_some_and(|extension| extension.eq_ignore_ascii_case("tmp"))
					&& fs::remove_file(&path).is_ok()
				{
					let _ = OpenOptions::new()
						.read(true)
						.open(&directory)
						.and_then(|file| file.sync_all());
				}
				continue;
			};
			if validate_local_name("sandbox name", sandbox_id).is_err() {
				continue;
			}
			let journal: RollbackJournal = serde_json::from_slice(&fs::read(&path)?)
				.map_err(|error| EngineError::engine(format!("reading rollback journal: {error}")))?;
			if journal.sandbox_id != sandbox_id {
				continue;
			}
			if let Some(record) = self.inner.registry.get(&journal.sandbox_id) {
				let source_is_live = record.status == "running"
					&& record
						.detail
						.get("rollback_source_token")
						.and_then(Value::as_str)
						.is_some_and(|token| token == journal.source_token)
					&& self.sandbox_is_running(&self.sandbox(&journal.sandbox_id))?;
				if rollback_replay_decision(source_is_live, false, None)
					== RollbackReplayDecision::AbortRetainSource
				{
					// The original process survived, but it may only resume
					// after the exact durable operation abort proves this
					// owner/epoch still controls the source.
					let handoff = self
						.inner
						.restore_handoff
						.lock()
						.as_ref()
						.and_then(Weak::upgrade);
					if self.inner.portable_history.is_some() && handoff.is_none() {
						return Err(EngineError::engine(
							"portable rollback replay requires an ownership handoff",
						));
					}
					if let Some(handoff) = handoff {
						handoff.abort_rollback(
							&journal.sandbox_id,
							&journal.portable_owner,
							journal.portable_owner_epoch,
						)?;
					}
					control_for_vm(&self.sandbox(&journal.sandbox_id))?.resume()?;
					self.inner.registry.cancel_transition(
						self.home(),
						&journal.sandbox_id,
						StateGeneration(journal.generation),
						LifecyclePhase::Running,
						"rollback replay retained original source",
					)?;
					self.finalize_rollback_journal(&journal)?;
					continue;
				}
				if self.inner.portable_history.is_some() {
					let ownership = {
						let mut ownership = self.inner.portable_ownership.lock();
						if ownership.is_none() {
							*ownership = PortableOwnership::connect(&self.inner.config)?;
						}
						ownership.as_ref().cloned().ok_or_else(|| {
							EngineError::engine("portable rollback ownership bridge disappeared")
						})?
					};
					let disposition = ownership.rollback_disposition(
						&journal.sandbox_id,
						&journal.portable_owner,
						journal.portable_owner_epoch,
					)?;
					let candidate_is_running = record.status == "running"
						&& self.sandbox_is_running(&self.sandbox(&journal.sandbox_id))?;
					if rollback_replay_decision(false, candidate_is_running, Some(&disposition))
						== RollbackReplayDecision::FinalizeRetainTarget
					{
						// PostgreSQL already atomically committed the replacement
						// as Running. This journal is only post-commit cleanup;
						// never tear a successful target down to restore safety.
						self.finalize_rollback_journal(&journal)?;
						continue;
					}
				}
				// Any other local candidate might be a target that launched
				// before durable finish. It is never proof of rollback success.
				if record.status == "running" {
					let _ = self.teardown(&record);
				}
				self.inner.registry.remove(&journal.sandbox_id);
			}
			// No matching original source remains. Recover the exact safety
			// point, never the requested target or a guessed newer history row.
			let recover = |point: &str| -> Result<()> {
				let cancellation = self.launch_cancellation(&journal.sandbox_id);
				Self::ensure_launch_not_cancelled(cancellation.as_deref())?;
				let (source, mut params) = self.open_recovery(&journal.sandbox_id, point)?;
				params.insert("name".to_owned(), json!(journal.sandbox_id));
				params.insert(
					"checkpoint_generation".to_owned(),
					json!(
						journal.checkpoint_generation.max(
							params
								.get("checkpoint_generation")
								.and_then(Value::as_u64)
								.unwrap_or(0),
						)
					),
				);
				ensure_checkpoint_template_present(&source.path)?;
				self
					.restore_from_template(params, &source.path, false)
					.map(|_| ())?;
				if let Err(error) = Self::ensure_launch_not_cancelled(cancellation.as_deref()) {
					if let Some(candidate) = self.inner.registry.get(&journal.sandbox_id) {
						let _ = self.teardown(&candidate);
						self.inner.registry.remove(&journal.sandbox_id);
					}
					return Err(error);
				}
				Ok(())
			};
			recover(&journal.safety_recovery_point)?;
			let mut record = self
				.inner
				.registry
				.get(&journal.sandbox_id)
				.ok_or_else(|| EngineError::engine("rollback replay restored no registry record"))?;
			if !record.detail.is_object() {
				record.detail = Value::Object(Map::new());
			}
			record
				.detail
				.as_object_mut()
				.expect("detail was materialized as an object")
				.insert("checkpoint_generation".to_owned(), json!(journal.checkpoint_generation));
			record.lifecycle = LifecycleState {
				desired:    LifecyclePhase::Running,
				observed:   LifecyclePhase::Running,
				generation: StateGeneration(journal.generation),
				failure:    None,
				operation:  Some(LifecycleOperation::Rollback {
					recovery_point: journal.target_recovery_point.clone(),
				}),
			};
			self.inner.registry.insert_persisted(self.home(), record)?;
			if let Some(handoff) = self
				.inner
				.restore_handoff
				.lock()
				.as_ref()
				.and_then(Weak::upgrade)
			{
				handoff.commit_rollback(
					&journal.sandbox_id,
					&journal.portable_owner,
					journal.portable_owner_epoch,
				)?;
			}
			self.complete_state_transition(
				&journal.sandbox_id,
				StateGeneration(journal.generation),
				LifecyclePhase::Running,
			)?;
			self.finalize_rollback_journal(&journal)?;
			self.clear_rollback_detail(&journal.sandbox_id);
		}
		Ok(())
	}

	fn replacement_checkpoint_generation(previous: &Value, restored: &Map<String, Value>) -> u64 {
		previous
			.get("checkpoint_generation")
			.and_then(Value::as_u64)
			.unwrap_or(0)
			.max(
				restored
					.get("checkpoint_generation")
					.and_then(Value::as_u64)
					.unwrap_or(0),
			)
	}

	fn restore_recovery_identity(
		&self,
		id: &str,
		recovery_point: &str,
		lease_owner: Option<(Arc<dyn OwnershipHandoff>, String, i64)>,
	) -> Result<Value> {
		let previous = self.get_record(id, false)?;
		let (source, mut params) = self.open_recovery(id, recovery_point)?;
		params.insert("name".to_owned(), json!(id));
		// A control-plane update made after the checkpoint must win when the
		// suspended identity is restored.
		if let Some(idle_timeout_secs) = previous.detail.get("idle_timeout_secs") {
			params.insert("idle_timeout_secs".to_owned(), idle_timeout_secs.clone());
		}
		// Snapshot params predate the durable capture allocation. Carry the
		// live monotonic counter into the *first* replacement record so a
		// crash after the new VM is inserted cannot rewind replica ordering.
		let checkpoint_generation =
			Self::replacement_checkpoint_generation(&previous.detail, &params);
		params.insert("checkpoint_generation".to_owned(), json!(checkpoint_generation));
		// Fully materialize and validate the selected replacement before any
		// destructive action against the currently runnable identity.
		ensure_checkpoint_template_present(&source.path)?;
		validate_network_restore(&params)?;
		// Acquire a newer fenced vote before destroying the old runtime. The
		// guard survives in RuntimeState until the caller's exact durable
		// Running commit succeeds.
		let mut volume_leases = lease_owner
			.map(|(handoff, owner, epoch)| {
				RestoreVolumeLeases::acquire(
					self.inner.net_runtime.handle().clone(),
					handoff,
					id,
					&owner,
					epoch,
					&mut params,
				)
			})
			.transpose()?;
		if previous.status == "running" {
			self.teardown(&previous)?;
		}
		let vm = self.sandbox(id);
		let _ = self.stop_sandbox(&vm, true);
		self.remove_sandbox(&vm)?;
		self.inner.registry.remove(id);
		let result = self.restore_from_template(params, &source.path, true);
		match result {
			Ok(_) => {
				if let Some(leases) = volume_leases.as_ref()
					&& let Err(error) = leases.persist(id)
				{
					// If candidate teardown cannot be proved, retain the guard
					// in RuntimeState; dropping it would release a vote while
					// this VMM may still be writing.
					self
						.inner
						.runtimes
						.lock()
						.entry(id.to_owned())
						.or_default()
						.restore_volume_leases = volume_leases.take();
					self.remove_restore_candidate(id)?;
					return Err(error);
				}
				let mut runtime = self.inner.runtimes.lock();
				let runtime = runtime.entry(id.to_owned()).or_default();
				runtime.restore_volume_leases = volume_leases.take();
				if let Some(guard) = source.guard {
					runtime.snapshot_source = Some(guard);
				}
				let mut restored = self.inner.registry.get(id).ok_or_else(|| {
					EngineError::engine("recovery restore completed without a registry record")
				})?;
				restored.created_at = previous.created_at;
				restored.source = previous.source;
				// Replacement launch starts at generation one, but it must not
				// erase an acquired lifecycle operation. Preserve the exact
				// pending generation/operation so its caller can observe (and
				// clear) that same transaction after the new VM is runnable.
				restored.lifecycle = Self::replacement_lifecycle(&previous.lifecycle);
				if let Some(detail) = restored.detail.as_object_mut() {
					detail.insert("status".to_owned(), json!("running"));
					detail.insert("desired_state".to_owned(), json!("running"));
					detail.insert("observed_state".to_owned(), json!("running"));
					detail.insert("state_generation".to_owned(), json!(restored.lifecycle.generation.0));
					let checkpoint_generation =
						Self::replacement_checkpoint_generation(&previous.detail, detail);
					detail.insert("checkpoint_generation".to_owned(), json!(checkpoint_generation));
					detail.insert("recovery_point".to_owned(), json!(recovery_point));
				}
				self
					.inner
					.registry
					.insert_persisted(self.home(), restored.clone())?;
				Ok(restored.view())
			},
			Err(error) => {
				let mut fallback = previous;
				if fallback.status != "suspended" {
					"stopped".clone_into(&mut fallback.status);
				}
				if let Some(detail) = fallback.detail.as_object_mut() {
					detail.insert("status".to_owned(), json!(fallback.status));
					detail.insert("observed_state".to_owned(), json!(fallback.status));
				}
				fs::create_dir_all(self.home().vm_dir(id))?;
				self
					.inner
					.registry
					.insert_persisted(self.home(), fallback)?;
				Err(error)
			},
		}
	}

	fn ensure_snapshot_target_available(&self, name: &str) -> Result<()> {
		validate_local_name("sandbox name", name)?;
		if self.inner.registry.get(name).is_some() || self.sandbox(name).dir().exists() {
			return Err(EngineError::busy(format!("sandbox already exists: {name}")));
		}
		Ok(())
	}

	fn rollback_snapshot_vm(&self, vm: &SandboxVm) {
		let name = vm.name();
		if let Some(mut runtime) = self.inner.runtimes.lock().remove(name)
			&& let Some(stop) = runtime.timeout_stop.take()
		{
			let _ = stop.send(());
		}
		self.inner.registry.remove(name);
		let _ = self.stop_sandbox(vm, false);
		let _ = self.remove_sandbox(vm);
	}

	fn launch_snapshot_vm(
		&self,
		snapshot: &str,
		snapshot_dir: &Path,
		name: String,
		mode: SnapshotLaunchMode,
		options: &ResolvedSnapshotOptions,
		s3_mounts: &[ResolvedS3Mount],
		snapshot_source: Option<Arc<TransientDir>>,
	) -> Result<(SandboxVm, VmRecord)> {
		self.ensure_snapshot_target_available(&name)?;
		// Launch performs authoritative validation; this early read only carries
		// snapshot capacity into the orchestration heartbeat.
		let snapshot_resources = vmm::snapshot::read_snapshot_metadata(snapshot_dir)
			.ok()
			.map(|image| (image.snapshot().cpus, image.snapshot().mem_mib));
		let vm = self.sandbox(&name);
		let result = (|| {
			let mut runtime = RuntimeState {
				secret_env: options.secret_env.clone(),
				env: options.env.clone(),
				workdir: options.workdir.clone(),
				snapshot_source,
				network_policy: NetworkPolicy {
					block_network:          Some(options.block_network.unwrap_or(true)),
					egress_allow:           options.egress_allow.clone(),
					egress_allow_domains:   options.egress_allow_domains.clone(),
					inbound_cidr_allowlist: options.inbound_cidr_allowlist.clone(),
				},
				identity_complete: true,
				..RuntimeState::default()
			};
			let network_params = SandboxCreate {
				block_network: options.block_network.unwrap_or(true),
				ports: options.ports.clone(),
				egress_allow: options.egress_allow.clone(),
				egress_allow_domains: options.egress_allow_domains.clone(),
				inbound_cidr_allowlist: options.inbound_cidr_allowlist.clone(),
				credentials: Some(options.credentials.clone()),
				owner_tenant: options.owner_tenant.clone(),
				encryption_key_id: options.encryption_key_id.clone(),
				..SandboxCreate::default()
			};
			let mut spec = match mode {
				SnapshotLaunchMode::Restore => LaunchSpec::restore(vm.api_sock(), snapshot_dir),
				SnapshotLaunchMode::Fork => LaunchSpec::fork_from(vm.api_sock(), snapshot_dir),
			}
			.with_agent_sock(vm.dir().join("agent.sock"));
			let base_disk = snapshot_dir.join("rootfs.img");
			if base_disk.is_file() {
				spec = spec.with_disk_overlay(base_disk, vm.dir().join("rootfs.img"));
			}
			if network_required(&network_params) {
				if cfg!(target_os = "macos") {
					reject_macos_host_network_features(&network_params)?;
					spec = spec.with_user_net();
					runtime.network_spec = Some(json!({
						"flavor": "user",
						"guest_config": user_net_guest_config(),
						"ports": [],
						"tunnels": {},
						"policy": policy_json(&runtime.network_policy),
					}));
					self.start_credential_gateway_for(
						&options.owner_tenant,
						&name,
						&options.credentials,
						&mut runtime,
						"127.0.0.1".parse().expect("loopback IP"),
						net::USER_NET_GATEWAY.parse().expect("user gateway IP"),
					)?;
					if !options.credentials.is_empty() {
						let port = runtime
							.credential_gateway
							.as_ref()
							.ok_or_else(|| EngineError::engine("credential gateway was not started"))?
							.port();
						spec = spec.with_restricted_user_net(port);
					}
				} else {
					let network = self.setup_network(&name, &network_params)?;
					let tap = network.guest_config.tap.clone();
					let host_ip = network.guest_config.host_ip.parse().map_err(|_| {
						EngineError::engine("allocated host gateway is not an IP address")
					})?;
					let tunnels = network.tunnels();
					runtime.network_spec = Some(json!({
						"flavor": "tap",
						"guest_config": network_guest_json(&network.guest_config),
						"ports": sorted_ports(options.ports.as_deref(), &tunnels),
						"tunnels": tunnels_json(&tunnels),
						"policy": policy_json(&runtime.network_policy),
					}));
					runtime.network = Some(network);
					self.start_credential_gateway_for(
						&options.owner_tenant,
						&name,
						&options.credentials,
						&mut runtime,
						host_ip,
						host_ip,
					)?;
					spec = spec.with_tap(tap);
				}
			}
			let needs_agent = options.agent
				|| !s3_mounts.is_empty()
				|| !options.credentials.is_empty()
				|| options.readiness_probe.is_some()
				|| options
					.command
					.as_ref()
					.is_some_and(|command| !command.is_empty());
			if matches!(mode, SnapshotLaunchMode::Restore) && needs_agent {
				spec = spec.with_console_agent();
			}
			if let Some(timeout_secs) = options.timeout_secs {
				spec = spec.with_timeout_secs(timeout_secs);
			}
			let (spec, s3_proxy) = self.with_s3_proxy(&vm, spec, s3_mounts)?;
			runtime.s3_proxy = s3_proxy;
			self.launch_sandbox(&vm, &spec)?;
			copy_agent_marker(snapshot_dir, vm.dir())?;
			if let Some(timeout_secs) = options.timeout_secs {
				runtime.timeout_stop = Some(start_timeout_watchdog(
					name.clone(),
					timeout_secs,
					Arc::clone(&self.inner.sandbox_runtime),
				));
			}
			if needs_agent {
				let agent = Self::agent_for_vm(&vm, AGENT_CONNECT_TIMEOUT)?;
				agent.ping(AGENT_CONNECT_TIMEOUT)?;
				Self::mount_s3_in_guest(&agent, s3_mounts)?;
				if let Some(probe) = &options.readiness_probe {
					Self::wait_until_ready(
						&agent,
						&runtime,
						probe,
						options.timeout_secs.map_or(300.0, |secs| secs as f64),
						None,
					)?;
				}
				runtime.agent = Some(agent);
			}

			let mut detail = vm.meta()?;
			detail.insert("block_network".to_owned(), json!(options.block_network.unwrap_or(true)));
			if let Some((cpus, memory)) = snapshot_resources {
				detail.insert("cpus".to_owned(), json!(cpus));
				detail.insert("memory".to_owned(), json!(memory));
			}
			detail.insert("workdir".to_owned(), json!(runtime.workdir));
			detail.insert("env_names".to_owned(), json!(runtime.env.keys().collect::<Vec<_>>()));
			detail.insert("secret_names".to_owned(), json!(options.secret_names));
			detail.insert("credential_names".to_owned(), json!(options.credentials));
			detail.insert("owner_tenant".to_owned(), json!(options.owner_tenant));
			detail.insert("encryption_key_id".to_owned(), json!(options.encryption_key_id));
			detail.insert("desired_state".to_owned(), json!("running"));
			detail.insert("observed_state".to_owned(), json!("running"));
			detail.insert("state_generation".to_owned(), json!(1));
			detail.insert("tags".to_owned(), json!(options.tags));
			detail.insert("timeout_secs".to_owned(), json!(options.timeout_secs));
			if let Some(idle_timeout_secs) = options.idle_timeout_secs {
				detail.insert("idle_timeout_secs".to_owned(), json!(idle_timeout_secs));
			}
			if let Some(activity_threshold_bytes) = options.activity_threshold_bytes {
				detail.insert("activity_threshold_bytes".to_owned(), json!(activity_threshold_bytes));
			}
			detail.insert("persistence".to_owned(), json!(options.persistence));
			detail.insert("s3_mounts".to_owned(), s3_mounts_meta(s3_mounts));
			detail.insert("network".to_owned(), json!(runtime.network_spec));
			if let Some(command) = &options.command {
				detail.insert("command".to_owned(), json!(command));
			}
			vm.save_meta(detail.clone())?;

			let runtime_identity = safe_runtime_identity(
				&runtime,
				std::iter::empty(),
				options.timeout_secs.map(|secs| secs as f64),
				Some(format!("{}:{snapshot}", match mode {
					SnapshotLaunchMode::Restore => "restore",
					SnapshotLaunchMode::Fork => "fork",
				})),
				Some(snapshot_dir.to_string_lossy().into_owned()),
			);
			self.inner.runtimes.lock().insert(name.clone(), runtime);
			if let Some(command) = options
				.command
				.as_ref()
				.filter(|command| !command.is_empty())
			{
				self.start_entry_command(name.clone(), command.clone())?;
			}
			let now = unix_time();
			let record = VmRecord {
				id: name.clone(),
				name: name.clone(),
				status: "running".to_owned(),
				pid: detail
					.get("pid")
					.and_then(Value::as_i64)
					.and_then(|pid| i32::try_from(pid).ok()),
				source: Some(format!("{}:{snapshot}", match mode {
					SnapshotLaunchMode::Restore => "restore",
					SnapshotLaunchMode::Fork => "fork",
				})),
				incarnation_epoch: detail
					.get("incarnation_epoch")
					.and_then(Value::as_i64)
					.unwrap_or(0),
				created_at: now,
				timeout: options.timeout_secs.map(|secs| secs as f64),
				detail: Value::Object(detail),
				tags: options.tags.clone(),
				last_active: now,
				last_network_active: now,
				persistence: options.persistence.clone(),
				terminated_at: None,
				error: None,
				lifecycle: LifecycleState {
					desired:    LifecyclePhase::Running,
					observed:   LifecyclePhase::Running,
					generation: StateGeneration(1),
					failure:    None,
					operation:  None,
				},
				runtime_identity,
			};
			self
				.inner
				.registry
				.insert_persisted(self.home(), record.clone())?;
			self.wake_maintenance();
			Ok((vm.clone(), record))
		})();
		if result.is_err() {
			self.rollback_snapshot_vm(&vm);
		}
		result
	}

	fn snapshot_machine(
		&self,
		vm: &SandboxVm,
		name: &str,
		keep_running: bool,
		disk_src: Option<&Path>,
		snapshot_root: &Path,
		track: bool,
	) -> Result<PathBuf> {
		self.snapshot_machine_while_paused(
			vm,
			name,
			keep_running,
			disk_src,
			snapshot_root,
			track,
			false,
			|_| Ok(()),
		)
	}

	fn snapshot_machine_while_paused<F>(
		&self,
		vm: &SandboxVm,
		name: &str,
		keep_running: bool,
		disk_src: Option<&Path>,
		snapshot_root: &Path,
		track: bool,
		hold_paused: bool,
		while_paused: F,
	) -> Result<PathBuf>
	where
		F: FnOnce(&Path) -> Result<()>,
	{
		validate_local_name("snapshot name", name)?;
		fs::create_dir_all(snapshot_root)?;
		let root = fs::canonicalize(snapshot_root)?;
		let dir = root.join(name);
		match fs::symlink_metadata(&dir) {
			Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
				return Err(EngineError::invalid(format!(
					"snapshot {name:?} is not a regular snapshot directory"
				)));
			},
			Ok(_) => {},
			Err(error) if error.kind() == io::ErrorKind::NotFound => {},
			Err(error) => return Err(error.into()),
		}
		let mut control = control_for_vm(vm)?;
		control.pause()?;
		// Copy all guest-writable state while devices and vCPUs are quiesced.
		let snapshot_result = control.snapshot(name, None, track);
		// The control server closes idle connections after five seconds; sealing an
		// encrypted snapshot can take longer, so resume over a fresh connection.
		drop(control);
		let snapshot_result = snapshot_result.and_then(|_| {
			if let Some(disk_src) = disk_src.filter(|path| path.is_file()) {
				fs::create_dir_all(&dir)?;
				fs::copy(disk_src, dir.join("rootfs.img"))?;
			}
			while_paused(&dir)?;
			Ok(Value::Null)
		});
		if let Err(error) = snapshot_result {
			let _ = control_for_vm(vm).and_then(|mut control| control.resume());
			return Err(error);
		}
		if hold_paused {
			// A production suspend keeps the precise captured state frozen until
			// its portable recovery point is committed.
		} else if keep_running {
			control_for_vm(vm)?.resume()?;
		} else {
			let _ = self.stop_sandbox(vm, true);
		}
		Ok(dir)
	}

	fn publish_event(&self, event_type: &str, mut data: Map<String, Value>) {
		let sequence = self.inner.event_sequence.fetch_add(1, Ordering::Relaxed) + 1;
		data.insert("type".to_owned(), json!(event_type));
		data.insert("sequence".to_owned(), json!(sequence));
		data.insert("ts".to_owned(), json!(unix_time()));
		let payload = Value::Object(data);
		let mut subscribers = self.inner.events.lock();
		subscribers.retain(|sender| sender.send(payload.clone()).is_ok());
	}

	fn publish_create_failure(&self, sid: &str, error: &EngineError) {
		self.publish_event(
			"failed",
			Map::from_iter([
				("id".to_owned(), json!(sid)),
				("name".to_owned(), json!(sid)),
				("status".to_owned(), json!("failed")),
				("code".to_owned(), json!(error.code.as_str())),
				("error".to_owned(), json!(&error.message)),
			]),
		);
	}

	fn publish_record_event(&self, event_type: &str, record: &VmRecord) {
		let status = match event_type {
			"paused" => "paused",
			"removed" => "removed",
			"ready" | "resumed" | "restore" | "fork" => "running",
			_ => &record.status,
		};
		let mut data = Map::from_iter([
			("id".to_owned(), json!(record.id)),
			("name".to_owned(), json!(record.name)),
			("status".to_owned(), json!(status)),
			("source".to_owned(), json!(record.source)),
			("tags".to_owned(), json!(record.tags)),
		]);
		if let Some(returncode) = record.detail.get("returncode").and_then(Value::as_i64) {
			data.insert("returncode".to_owned(), json!(returncode));
		}
		if let Some(reason) = record
			.detail
			.get("terminated_reason")
			.and_then(Value::as_str)
		{
			data.insert("reason".to_owned(), json!(reason));
		}
		if let Some(error) = &record.error {
			data.insert("error".to_owned(), json!(error));
		}
		self.publish_event(event_type, data);
	}

	fn inc_counter(&self, name: &str) {
		match name {
			"created" => {
				self.inner.counters.created.fetch_add(1, Ordering::Relaxed);
			},
			"terminated" => {
				self
					.inner
					.counters
					.terminated
					.fetch_add(1, Ordering::Relaxed);
			},
			"idle_reaped" => {
				self
					.inner
					.counters
					.idle_reaped
					.fetch_add(1, Ordering::Relaxed);
			},
			"exec" => {
				self.inner.counters.exec.fetch_add(1, Ordering::Relaxed);
			},
			"file_read" => {
				self
					.inner
					.counters
					.file_read
					.fetch_add(1, Ordering::Relaxed);
			},
			"file_write" => {
				self
					.inner
					.counters
					.file_write
					.fetch_add(1, Ordering::Relaxed);
			},
			"file_delete" => {
				self
					.inner
					.counters
					.file_delete
					.fetch_add(1, Ordering::Relaxed);
			},
			"snapshot" => {
				self.inner.counters.snapshot.fetch_add(1, Ordering::Relaxed);
			},
			"auth_failed" => {
				self
					.inner
					.counters
					.auth_failed
					.fetch_add(1, Ordering::Relaxed);
			},
			_ => {},
		}
	}

	#[allow(
		clippy::unnecessary_wraps,
		reason = "mesh adapters expect Engine helpers to report errors uniformly"
	)]
	pub(crate) fn mesh_owned_ids(&self) -> Result<Vec<String>> {
		Ok(self
			.inner
			.registry
			.list()
			.into_iter()
			.filter(|record| record.status != "terminated")
			.map(|record| record.id)
			.collect())
	}

	/// Claim an expired exact durable suspended marker and project it locally
	/// without serving it. The ordinary `resume` path then continues the exact
	/// `resuming` epoch and performs the recovery launch.
	pub(crate) fn mesh_adopt_suspended_marker(&self, sid: &str) -> Result<()> {
		let portable = {
			let mut ownership = self.inner.portable_ownership.lock();
			if ownership.is_none() {
				*ownership = PortableOwnership::connect(&self.inner.config)?;
			}
			ownership.as_ref().cloned().ok_or_else(|| {
				EngineError::unsupported("portable suspended-marker adoption is unavailable")
			})?
		};
		let marker = portable.suspend_marker(sid)?.ok_or_else(|| {
			EngineError::not_found("suspended sandbox has no durable recovery point")
		})?;
		if marker.state != "suspended" {
			return Err(EngineError::busy(format!("suspended sandbox '{sid}' is {}", marker.state)));
		}
		let handoff = self
			.inner
			.restore_handoff
			.lock()
			.as_ref()
			.and_then(Weak::upgrade)
			.ok_or_else(|| EngineError::engine("portable resume requires an ownership handoff"))?;
		let claimed = portable.claim_suspend_marker(&marker)?;
		let lease = claimed.lease;
		let current = portable
			.suspend_marker(sid)?
			.ok_or_else(|| EngineError::busy("suspended marker disappeared after claim"))?;
		if current.state != "suspended"
			|| current.point != marker.point
			|| current.generation != marker.generation
			|| current.owner != lease.owner_node
			|| current.epoch != lease.epoch
		{
			let _ = portable.abort(&lease);
			return Err(EngineError::busy("suspended marker changed while being claimed"));
		}
		let begin_result = handoff.begin_restore(sid, &lease.owner_node, lease.epoch);
		let current = portable.suspend_marker(sid)?;
		match (begin_result, current) {
			(_, Some(current))
				if current.state == "resuming"
					&& current.point == marker.point
					&& current.generation == marker.generation
					&& current.owner == lease.owner_node
					&& current.epoch == lease.epoch =>
			{
				// The server can commit before losing the response. The exact
				// durable resuming row is authoritative and safely continues.
				if let Some(existing) = self.inner.registry.get(sid) {
					if existing.status != "suspended"
						|| existing
							.detail
							.get("_mesh_suspended_projection")
							.and_then(Value::as_bool)
							!= Some(true)
					{
						return Err(EngineError::busy(
							"cannot replace a serving sandbox with a suspended projection",
						));
					}
					self.inner.registry.remove(sid);
				}
				self.mesh_install_suspended_placeholder(&claimed.record, &current)
			},
			(Err(error), Some(current))
				if current.state == "suspended"
					&& current.point == marker.point
					&& current.generation == marker.generation
					&& current.owner == lease.owner_node
					&& current.epoch == lease.epoch =>
			{
				let _ = portable.abort(&lease);
				Err(error)
			},
			(Err(error), _) => Err(error),
			(Ok(()), _) => {
				Err(EngineError::busy("suspended marker changed while entering resuming state"))
			},
		}
	}

	pub(crate) fn mesh_list_views(&self) -> Result<Vec<Value>> {
		<Self as EngineApi>::list(self, None)
	}

	pub(crate) fn mesh_has_sandbox(&self, sid: &str) -> bool {
		self
			.inner
			.registry
			.get(sid)
			.is_some_and(|record| record.status != "terminated")
	}

	pub(crate) fn mesh_get_view(&self, sid: &str) -> Result<Value> {
		Ok(self.get_record(sid, false)?.view())
	}

	/// Project a durable cluster suspension into this node's local registry
	/// without creating a VM or trusting host-local history metadata.
	pub(crate) fn mesh_install_suspended_placeholder(
		&self,
		record: &CreateRecord,
		marker: &SuspensionMarker,
	) -> Result<()> {
		if self.inner.registry.get(&record.sid).is_some() {
			return Ok(());
		}
		let mut detail = record.params.clone();
		for key in ["secrets", "secret", "password", "token", "access_key", "secret_key"] {
			detail.remove(key);
		}
		detail.insert("name".to_owned(), json!(record.sid));
		detail.insert("status".to_owned(), json!("suspended"));
		detail.insert("suspend_recovery_point".to_owned(), json!(marker.point));
		detail.insert("suspend_generation".to_owned(), json!(marker.generation));
		detail.insert("suspend_owner".to_owned(), json!(marker.owner));
		detail.insert("_mesh_suspended_projection".to_owned(), Value::Bool(true));
		detail.insert("suspend_owner_epoch".to_owned(), json!(marker.epoch));
		let mut local = VmRecord::new(&record.sid, &record.sid, "suspended");
		local.source = detail
			.get("image")
			.and_then(Value::as_str)
			.map(str::to_owned);
		local.created_at = record.created_at;
		local.timeout = detail.get("timeout").and_then(Value::as_f64);
		local.tags = detail
			.get("tags")
			.and_then(Value::as_object)
			.map(|tags| {
				tags
					.iter()
					.filter_map(|(key, value)| {
						value.as_str().map(|value| (key.clone(), value.to_owned()))
					})
					.collect()
			})
			.unwrap_or_default();
		local.detail = Value::Object(detail);
		local.lifecycle = LifecycleState {
			desired:    LifecyclePhase::Suspended,
			observed:   LifecyclePhase::Suspended,
			generation: StateGeneration(marker.generation),
			operation:  None,
			failure:    None,
		};
		self.inner.registry.insert_persisted(self.home(), local)
	}

	pub(crate) fn mesh_checkpoint_age_sec(&self, sid: &str) -> Option<f64> {
		let record = self.inner.registry.get(sid)?;
		let checkpoint_ts = record.detail.get("checkpoint_ts")?.as_f64()?;
		Some((unix_time() - checkpoint_ts).max(0.0))
	}

	#[allow(
		clippy::unnecessary_wraps,
		reason = "mesh adapters expect Engine lookup helpers to report errors uniformly"
	)]
	pub(crate) fn mesh_find_by_idempotency_key(&self, key: &str) -> Result<Option<Value>> {
		let Some(name) = self.inner.registry.find_by_idempotency_key(key) else {
			return Ok(None);
		};
		Ok(self.inner.registry.get(&name).map(|record| record.view()))
	}

	pub(crate) fn mesh_record_idempotency(&self, sid: &str, key: &str) -> Result<()> {
		self.inner.registry.record_idempotency(sid, key);
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([("idempotency_key".to_owned(), json!(key))]),
		)
	}

	pub(crate) fn mesh_record_create_epoch(&self, sid: &str, epoch: i64) -> Result<()> {
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([("_mesh_create_epoch".to_owned(), json!(epoch))]),
		)
	}

	/// Attach granted writable-volume lease metadata to a live record, keyed by
	/// volume name and merged over any existing leases (Python
	/// `Engine.record_volume_leases` parity).
	pub(crate) fn mesh_record_volume_leases(&self, sid: &str, leases: Vec<Value>) -> Result<()> {
		let mut lease_map = Map::new();
		for lease in leases {
			let Some(volume) = lease
				.get("volume")
				.and_then(Value::as_str)
				.filter(|volume| !volume.is_empty())
			else {
				continue;
			};
			lease_map.insert(volume.to_owned(), lease.clone());
		}
		if lease_map.is_empty() {
			return Ok(());
		}
		let mut merged = self
			.inner
			.registry
			.get(sid)
			.and_then(|record| {
				record
					.detail
					.get("volume_leases")
					.and_then(Value::as_object)
					.cloned()
			})
			.unwrap_or_default();
		merged.extend(lease_map);
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([("volume_leases".to_owned(), Value::Object(merged))]),
		)
	}

	/// Fail closed when runtime lease reconciliation cannot prove a local
	/// candidate is still alive.
	pub(crate) fn candidate_is_running(&self, sid: &str) -> bool {
		self.sandbox_is_running(&self.sandbox(sid)).unwrap_or(false)
	}

	/// Rearm the VMM owner watchdog after a mesh ownership renewal without
	/// counting the renewal as guest activity.
	pub(crate) fn mesh_rearm_owner_lease(&self, sid: &str, secs: u64) -> Result<()> {
		let record = self.get_record(sid, false)?;
		if record.status != "running" || !self.sandbox_is_running(&self.sandbox(sid))? {
			return Err(EngineError::busy(format!(
				"sandbox '{sid}' is not a running local candidate"
			)));
		}
		control_for_vm(&self.sandbox(sid))?.rearm_owner_lease(secs)?;
		Ok(())
	}

	/// Forget lease metadata after all matching votes have been released.
	/// A no-op for already-removed records (fencing removes before releasing).
	#[allow(
		clippy::unnecessary_wraps,
		reason = "lease cleanup participates in fallible Engine cleanup flows"
	)]
	pub(crate) fn mesh_clear_volume_leases(&self, sid: &str) -> Result<()> {
		if self.inner.registry.get(sid).is_none() {
			return Ok(());
		}
		let _ = self
			.inner
			.registry
			.update_detail_persisted(self.home(), sid, |detail| {
				detail.remove("volume_leases");
			});
		Ok(())
	}

	/// Create a mesh migration candidate with its VMM paused before any guest
	/// instruction or guest-agent request. It remains a local, non-serving
	/// record until `mesh_activate_candidate` completes after the caller's
	/// durable ownership commit.
	pub(crate) fn mesh_create_from_params_paused(
		&self,
		mut params: Map<String, Value>,
	) -> Result<Value> {
		let s3_restore_tags = restore_s3_mount_params(&mut params)?;
		let mut request = crate::mesh::runtime::sandbox_create_from_mesh_params(params)?;
		request.s3_restore_tags = s3_restore_tags;
		let mut plan = self.prepare_create(request)?;
		let sid = plan.sid.clone();
		let source = plan
			.params
			.image
			.clone()
			.or_else(|| Some(plan.template_dir.to_string_lossy().into_owned()));
		let detail = serde_json::to_value(&plan.params)
			.ok()
			.and_then(|value| value.as_object().cloned())
			.unwrap_or_default();
		let (vm, runtime) = self.launch_create(&mut plan, true)?;
		let meta = vm.meta()?;
		let now = unix_time();
		let record = VmRecord {
			id: sid.clone(),
			name: sid.clone(),
			status: "paused".to_owned(),
			pid: meta
				.get("pid")
				.and_then(Value::as_i64)
				.and_then(|pid| i32::try_from(pid).ok()),
			source,
			incarnation_epoch: detail
				.get("incarnation_epoch")
				.and_then(Value::as_i64)
				.unwrap_or(0),
			created_at: now,
			timeout: None,
			detail: Value::Object(detail),
			tags: HashMap::new(),
			last_active: now,
			last_network_active: now,
			persistence: plan.params.persistence.clone().unwrap_or_default(),
			terminated_at: None,
			error: None,
			lifecycle: LifecycleState {
				desired:    LifecyclePhase::Running,
				observed:   LifecyclePhase::Paused,
				generation: StateGeneration(1),
				failure:    None,
				operation:  None,
			},
			runtime_identity: SafeRuntimeIdentity::default(),
		};
		if let Err(error) = self
			.inner
			.registry
			.insert_persisted(self.home(), record.clone())
		{
			let mut runtime = runtime;
			self.rollback_uncommitted_runtime(&vm, &mut runtime, false);
			return Err(error);
		}
		self.inner.runtimes.lock().insert(sid, runtime);
		Ok(record.view())
	}

	/// Resume and finish exactly one locally staged candidate. The persisted PID
	/// fences both a reused sandbox directory and a stale reconciliation retry.
	pub(crate) fn mesh_activate_candidate(&self, sid: &str) -> Result<()> {
		let mut record = self.inner.registry.get(sid).ok_or_else(|| {
			EngineError::not_found(format!("sandbox '{sid}' has no local candidate"))
		})?;
		if record.status == "running" {
			return Ok(());
		}
		if record.status != "paused" {
			return Err(EngineError::busy(format!("sandbox '{sid}' is not a paused local candidate")));
		}
		let vm = self.sandbox(sid);
		let actual_pid = vm
			.meta()?
			.get("pid")
			.and_then(Value::as_i64)
			.and_then(|pid| i32::try_from(pid).ok());
		if record.pid.is_none() || record.pid != actual_pid {
			return Err(EngineError::busy(format!(
				"sandbox '{sid}' candidate PID no longer matches its durable fence"
			)));
		}
		let mut runtime = self
			.inner
			.runtimes
			.lock()
			.remove(sid)
			.ok_or_else(|| EngineError::busy("paused candidate lost its pending runtime setup"))?;
		let plan = runtime
			.pending_setup
			.take()
			.ok_or_else(|| EngineError::busy("paused candidate has already been activated"))?;
		let result = (|| -> Result<()> {
			control_for_vm(&vm)?.resume()?;
			let cancellation = self.launch_cancellation(sid);
			let agent =
				Self::agent_for_vm_cancellable(&vm, AGENT_CONNECT_TIMEOUT, cancellation.as_deref())?;
			if let Some(timeout_secs) = plan.timeout_secs
				&& runtime.timeout_stop.is_none()
			{
				runtime.timeout_stop = Some(start_timeout_watchdog(
					vm.name().to_owned(),
					timeout_secs,
					Arc::clone(&self.inner.sandbox_runtime),
				));
			}
			for volume in &plan.volume_specs {
				agent.mount(
					&volume.tag,
					Path::new(&volume.mountpoint),
					volume.read_only,
					"virtiofs",
					AGENT_REQUEST_TIMEOUT,
				)?;
			}
			Self::mount_s3_in_guest(&agent, &plan.s3_specs)?;
			if let Some(network) = &runtime.network {
				let gc = &network.guest_config;
				agent.net_config(
					&gc.guest_ip,
					gc.prefix,
					&gc.host_ip,
					Some(&gc.dns),
					AGENT_REQUEST_TIMEOUT,
				)?;
			} else if network_required(&plan.params) {
				let gc = user_net_guest_config();
				let dns = net::USER_NET_DNS
					.iter()
					.map(|dns| (*dns).to_owned())
					.collect::<Vec<_>>();
				agent.net_config(
					gc["guest_ip"].as_str().unwrap_or(net::USER_NET_GUEST_IP),
					gc["prefix"]
						.as_u64()
						.unwrap_or_else(|| u64::from(net::USER_NET_PREFIX)) as u8,
					gc["host_ip"].as_str().unwrap_or(net::USER_NET_GATEWAY),
					Some(&dns),
					AGENT_REQUEST_TIMEOUT,
				)?;
			}
			if let Some(probe) = &plan.params.readiness_probe {
				Self::wait_until_ready(
					&agent,
					&runtime,
					probe,
					plan.params.timeout.unwrap_or(300.0),
					cancellation.as_deref(),
				)?;
			}
			runtime.agent = Some(agent);
			Ok(())
		})();
		if let Err(error) = result {
			runtime.pending_setup = Some(plan);
			self.inner.runtimes.lock().insert(sid.to_owned(), runtime);
			return Err(error);
		}
		"running".clone_into(&mut record.status);
		record.lifecycle.observed = LifecyclePhase::Running;
		record.lifecycle.desired = LifecyclePhase::Running;
		record.runtime_identity = safe_runtime_identity(
			&runtime,
			plan.secrets.iter().map(|secret| secret.name.clone()),
			plan.timeout_secs.map(|secs| secs as f64),
			plan.params.image.clone(),
			Some(plan.template_dir.to_string_lossy().into_owned()),
		);
		self.inner.registry.insert_persisted(self.home(), record)?;
		self.inner.runtimes.lock().insert(sid.to_owned(), runtime);
		self.wake_maintenance();
		Ok(())
	}

	/// Running records that both hold writable volumes and have lease
	/// metadata: `(name, volume_leases, writable volume names)` per record.
	pub(crate) fn mesh_volume_lease_records(
		&self,
	) -> Vec<(String, Map<String, Value>, Vec<String>)> {
		self
			.inner
			.registry
			.list()
			.into_iter()
			.filter(|record| record.status == "running")
			.filter_map(|record| {
				let leases = record
					.detail
					.get("volume_leases")
					.and_then(Value::as_object)
					.cloned()?;
				let writable = writable_volume_names_from_detail(&record.detail);
				if writable.is_empty() {
					return None;
				}
				Some((record.name, leases, writable))
			})
			.collect()
	}

	/// Stop a sandbox after a writable-volume lease renewal failure.
	pub(crate) fn mesh_stop_sandbox(&self, sid: &str) -> Result<()> {
		let _ = <Self as EngineApi>::stop(self, sid)?;
		Ok(())
	}

	pub(crate) fn mesh_set_ha_metadata(
		&self,
		sid: &str,
		ha: &str,
		restart_policy: &str,
	) -> Result<()> {
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([
				("ha".to_owned(), json!(ha)),
				("restart_policy".to_owned(), json!(restart_policy)),
			]),
		)
	}

	pub(crate) fn mesh_create_from_params(&self, mut params: Map<String, Value>) -> Result<Value> {
		let s3_restore_tags = restore_s3_mount_params(&mut params)?;
		let mut request = crate::mesh::runtime::sandbox_create_from_mesh_params(params)?;
		request.s3_restore_tags = s3_restore_tags;
		<Self as EngineApi>::create(self, request)
	}

	/// Eligibility checks + restore params shared by every peer checkpoint.
	///
	/// Merges the persisted record detail with the *live* runtime state (env,
	/// secrets, network spec) a peer needs to re-create the sandbox. Raises
	/// `Unsupported` for sandboxes that cannot move hosts (`fs_dir` shares,
	/// rehydrated records, missing network state).
	fn mesh_checkpoint_params(&self, sid: &str) -> Result<(VmRecord, Map<String, Value>)> {
		let record = self.get_record(sid, false)?;
		let detail = record.detail.as_object().cloned().unwrap_or_default();
		if detail
			.get("fs_dir")
			.and_then(Value::as_str)
			.is_some_and(|dir| !dir.is_empty())
		{
			return Err(EngineError::unsupported(
				"a sandbox with an fs_dir share cannot migrate; the share is host-local",
			));
		}
		// Live restore state exists only for in-process sandboxes; one
		// rehydrated after a daemon restart has lost the env/secret/network
		// identity needed to restore it elsewhere.
		let (env, secret_env, network_spec, workdir) = {
			let runtimes = self.inner.runtimes.lock();
			let Some(runtime) = runtimes.get(&record.name) else {
				return Err(EngineError::unsupported(
					"migration requires a live in-process sandbox; one rehydrated after a daemon \
					 restart has lost the identity needed to move it",
				));
			};
			if !runtime.identity_complete {
				return Err(EngineError::unsupported(
					"migration requires complete in-process restore identity",
				));
			}
			(
				runtime.env.clone(),
				runtime.secret_env.clone(),
				runtime.network_spec.clone(),
				runtime.workdir.clone(),
			)
		};
		let mut params = detail;
		params.insert("state_generation".to_owned(), json!(record.lifecycle.generation.0));
		params.insert("name".to_owned(), json!(sid));
		params.insert("env".to_owned(), json!(env));
		if let Some(workdir) = workdir {
			params.insert("workdir".to_owned(), json!(workdir));
		}
		if !secret_env.is_empty() {
			// Carried over the bearer-authenticated cluster channel, like the
			// memory image; the peer keeps them in memory only.
			params.insert("secrets".to_owned(), json!([{ "name": "carried", "values": secret_env }]));
		}
		let block_network = params
			.get("block_network")
			.and_then(Value::as_bool)
			.unwrap_or(false);
		if !block_network {
			let network = network_spec
				.filter(|spec| spec.is_object())
				.or_else(|| {
					params
						.get("network")
						.filter(|spec| spec.is_object())
						.cloned()
				})
				.ok_or_else(|| {
					EngineError::unsupported(format!(
						"networked sandbox '{sid}' is missing live host-network restore state"
					))
				})?;
			if let Some(ports) = network.get("ports").filter(|ports| ports.is_array()) {
				params.insert("ports".to_owned(), ports.clone());
			}
			params.insert("network".to_owned(), network);
		}
		for key in [
			"status",
			"pid",
			"api_sock",
			"agent_sock",
			"console_log",
			"idempotency_key",
			"env_names",
			"secret_names",
		] {
			params.remove(key);
		}
		Ok((record, params))
	}

	/// Allocate a durable monotonic checkpoint sequence. Replica metadata uses
	/// this instead of wall-clock ordering so a late transfer cannot overwrite
	/// a newer recovery generation.
	fn next_checkpoint_generation(&self, sid: &str) -> Result<u64> {
		let record = self
			.inner
			.registry
			.update_detail_persisted(self.home(), sid, |detail| {
				let next = detail
					.get("checkpoint_generation")
					.and_then(Value::as_u64)
					.unwrap_or(0)
					.saturating_add(1);
				detail.insert("checkpoint_generation".to_owned(), json!(next));
			})?;
		Ok(record
			.detail
			.get("checkpoint_generation")
			.and_then(Value::as_u64)
			.unwrap_or(0))
	}

	/// Build a peer-pullable checkpoint + restore params for replication and
	/// migration pre-copy. The source VM keeps running (it pauses only for
	/// the dump); volume data is captured into the checkpoint before the
	/// content digest so it travels with the bundle.
	fn mesh_checkpoint_for(
		&self,
		sid: &str,
		kind: &str,
		track: bool,
	) -> Result<crate::mesh::reconciler::ReplicatePreparation> {
		let capture_lock = self.capture_lock(sid);
		let _capture_guard = capture_lock.acquire();
		self.mesh_checkpoint_while_capture_held(sid, kind, track)
	}

	fn mesh_checkpoint_while_capture_held(
		&self,
		sid: &str,
		kind: &str,
		track: bool,
	) -> Result<crate::mesh::reconciler::ReplicatePreparation> {
		let lifecycle = self.get_record(sid, false)?;
		if lifecycle.status != "running" || !lifecycle.lifecycle.is_converged() {
			return Err(EngineError::busy("sandbox lifecycle is not steady for mesh checkpoint"));
		}
		let t0 = std::time::Instant::now();
		let (record, params) = self.mesh_checkpoint_params(sid)?;
		let snapshot = format!("{kind}-{sid}-{}", unix_millis());
		let vm = self.sandbox(sid);
		let disk = vm.rootfs_img().ok().filter(|path| path.is_file());
		let mut snapshot_ms = 0;
		let snapshot_dir = self.snapshot_machine_while_paused(
			&vm,
			&snapshot,
			true,
			disk.as_deref(),
			&self.snapshot_root(),
			track,
			false,
			|snapshot_dir| {
				snapshot_ms = t0.elapsed().as_millis();
				stamp_checkpoint_rootfs(self.home(), snapshot_dir, record.detail.as_object())?;
				stamp_checkpoint_marker(snapshot_dir, record.detail.as_object())?;
				capture_checkpoint_volumes(self, sid, snapshot_dir, record.detail.get("volumes"))?;
				ensure_checkpoint_template_present(snapshot_dir)
			},
		)?;
		let stamp_ms = t0.elapsed().as_millis() - snapshot_ms;
		let digest = image::cas::snapshot_digest(&snapshot_dir)?;
		image::cas::index_template(&snapshot_dir, Some(&digest))?;
		let mut cleanup = CheckpointCleanup::new(digest.clone(), snapshot_dir.clone());
		tracing::info!(
			sid,
			kind,
			snapshot_ms = snapshot_ms as u64,
			stamp_ms = stamp_ms as u64,
			index_ms = (t0.elapsed().as_millis() - snapshot_ms - stamp_ms) as u64,
			"checkpoint timings"
		);
		let checkpoint_generation = self.next_checkpoint_generation(sid)?;
		cleanup.disarm();
		Ok(crate::mesh::reconciler::ReplicatePreparation {
			digest,
			snapshot_dir,
			params: Value::Object(params),
			checkpoint_generation,
		})
	}

	/// Non-destructively checkpoint a live sandbox for HA replication; the
	/// source VM keeps running.
	pub(crate) fn mesh_replicate_prepare(
		&self,
		sid: &str,
	) -> Result<crate::mesh::reconciler::ReplicatePreparation> {
		let prep = self.mesh_checkpoint_for(sid, "replica", false)?;
		let cleanup = CheckpointCleanup::new(prep.digest.clone(), prep.snapshot_dir.clone());
		self.mesh_update_detail_fields(
			sid,
			Map::from_iter([("checkpoint_ts".to_owned(), json!(unix_time()))]),
		)?;
		self.inner.pending_replica_exports.lock().insert(
			sid.to_owned(),
			Arc::new(ReplicaExport {
				digest:       prep.digest.clone(),
				snapshot_dir: prep.snapshot_dir.clone(),
				_cleanup:     cleanup,
				object_key:   Mutex::new(None),
			}),
		);
		Ok(prep)
	}

	/// Live-migration phase 1 (pre-copy): checkpoint the running sandbox for
	/// a peer pull; the source keeps running. Follow with
	/// [`Self::mesh_migrate_finalize`] once the target holds the bulk image.
	/// The caller must retain the guard from
	/// [`Self::try_acquire_running_capture`] across both migration phases.
	pub(crate) fn mesh_migrate_precopy(
		&self,
		sid: &str,
	) -> Result<crate::mesh::reconciler::ReplicatePreparation> {
		self.mesh_checkpoint_while_capture_held(sid, "migrate", true)
	}

	/// Atomically persist the source-side migration recovery journal.  The
	/// final delta must be recoverable before its paused source is stopped.
	pub(crate) fn mesh_migration_cleanup_persist(
		&self,
		cleanup: &MigrationCleanupWire,
	) -> Result<()> {
		if cleanup.sid.is_empty()
			|| cleanup.base_dir.is_empty()
			|| cleanup.base_digest.is_empty()
			|| cleanup.delta_dir.is_empty()
			|| cleanup.delta_digest.is_empty()
		{
			return Err(EngineError::invalid("incomplete migration cleanup journal"));
		}
		let dir = self
			.home()
			.security_dir()
			.join("runtime")
			.join("migration-cleanup");
		fs::create_dir_all(&dir)?;
		let path = dir.join(format!("{}.json", cleanup.sid));
		let temporary = dir.join(format!(".{}.{}.tmp", cleanup.sid, uuid::Uuid::new_v4()));
		let bytes = serde_json::to_vec(cleanup)
			.map_err(|error| EngineError::engine(format!("serializing migration cleanup: {error}")))?;
		let result = (|| -> Result<()> {
			let mut file = OpenOptions::new()
				.write(true)
				.create_new(true)
				.mode(0o600)
				.open(&temporary)?;
			file.write_all(&bytes)?;
			file.write_all(b"\n")?;
			file.sync_all()?;
			fs::rename(&temporary, &path)?;
			OpenOptions::new().read(true).open(&dir)?.sync_all()?;
			Ok(())
		})();
		if result.is_err() {
			let _ = fs::remove_file(&temporary);
		}
		result
	}

	/// Live-migration phase 2: pause the source and capture a delta
	/// checkpoint against the pre-copy `base_dir` — changed RAM pages (the
	/// VMM's delta snapshot), changed disk blocks (`rootfs-delta.bin`), and
	/// fresh volume trees — then stop the source exactly at the captured
	/// state.
	///
	/// Any capture failure resumes the source and removes the partial delta;
	/// once every artifact is durable the source is stopped and the returned
	/// checkpoint is the sole authority. Follow with
	/// [`Self::mesh_migrate_commit`] once the target confirms, or
	/// [`Self::mesh_migrate_abort`] to restore the source locally.
	/// The caller must still hold the capture guard acquired before pre-copy.
	pub(crate) fn mesh_migrate_finalize(
		&self,
		sid: &str,
		base_dir: &Path,
		mut cleanup: MigrationCleanupWire,
	) -> Result<(crate::mesh::reconciler::ReplicatePreparation, MigrationCleanupWire)> {
		let lifecycle = self.get_record(sid, false)?;
		if lifecycle.status != "running" || !lifecycle.lifecycle.is_converged() {
			return Err(EngineError::busy("sandbox lifecycle is not steady for migration checkpoint"));
		}
		let (record, params) = self.mesh_checkpoint_params(sid)?;
		let base_name = base_dir
			.file_name()
			.and_then(|name| name.to_str())
			.ok_or_else(|| {
				EngineError::engine(format!(
					"pre-copy checkpoint {} has no usable directory name",
					base_dir.display()
				))
			})?;
		let snapshot = format!("migrate-{sid}-{}", unix_millis());
		let dir = self.snapshot_dir(&snapshot);
		let vm = self.sandbox(sid);
		let disk = vm.rootfs_img().ok().filter(|path| path.is_file());
		let mut control = control_for_vm(&vm)?;
		control.pause()?;
		let captured = (|| {
			control.snapshot(&snapshot, Some(base_name), false)?;
			if let Some(disk) = disk.as_deref() {
				let base_rootfs = base_dir.join("rootfs.img");
				if base_rootfs.is_file() {
					diskdelta::write_disk_delta(
						&base_rootfs,
						disk,
						&dir.join(diskdelta::DISK_DELTA_FILE),
					)?;
				}
			}
			capture_checkpoint_volumes(self, sid, &dir, record.detail.get("volumes"))?;
			stamp_checkpoint_marker(&dir, record.detail.as_object())?;
			let digest = image::cas::snapshot_digest(&dir)?;
			image::cas::index_template(&dir, Some(&digest))?;
			Ok(digest)
		})();
		let digest = match captured {
			Ok(digest) => digest,
			Err(err) => {
				// The source must keep running when finalize fails; a partial
				// delta is useless without the stop that never came.
				let _ = control.resume();
				let _ = fs::remove_dir_all(&dir);
				return Err(err);
			},
		};
		cleanup.delta_dir = dir.to_string_lossy().into_owned();
		cleanup.delta_digest.clone_from(&digest);
		if let Err(error) = self.mesh_migration_cleanup_persist(&cleanup) {
			// The journal is the recovery authority for a stopped source.  A
			// failed write therefore leaves the source running and drops the
			// unusable final checkpoint.
			let _ = control.resume();
			let _ = fs::remove_dir_all(&dir);
			return Err(error);
		}
		drop(control);
		// The journal is fsynced before this teardown, so a process death can
		// always abort/recover the exact paused delta.
		let rc = self.teardown(&record)?;
		self.persist_status(sid, "stopped", rc, None)?;
		Ok((
			crate::mesh::reconciler::ReplicatePreparation {
				digest,
				snapshot_dir: dir,
				params: Value::Object(params),
				checkpoint_generation: self.next_checkpoint_generation(sid)?,
			},
			cleanup,
		))
	}

	/// Finalize a successful migration: drop the stopped source, its local
	/// record, and both transient checkpoints (pre-copy base + final delta).
	/// The local record MUST go — the mesh router treats any local record as
	/// owned-here and would refuse to proxy to the new owner — so the
	/// registry entry is force-dropped even if teardown was partial.
	pub(crate) fn mesh_migrate_activate_target(
		&self,
		sid: &str,
		_expected_epoch: i64,
	) -> Result<()> {
		self.mesh_activate_candidate(sid)
	}

	pub(crate) fn mesh_migrate_commit(
		&self,
		sid: &str,
		base_dir: &Path,
		base_digest: &str,
		delta_dir: &Path,
		delta_digest: &str,
	) -> Result<()> {
		if self.remove_local_candidate(sid).is_err() {
			self.inner.registry.remove(sid);
		}
		self.mesh_drop_checkpoint(delta_digest, delta_dir, true)?;
		self.mesh_drop_checkpoint(base_digest, base_dir, true)
	}

	/// Roll back a failed live migration by restoring the source locally.
	/// The source was stopped exactly at the delta checkpoint, so re-creating
	/// it from that checkpoint resumes the VM with no lost work. The VM's
	/// final disk image still exists locally and is adopted as the
	/// checkpoint's `rootfs.img` (cheaper than replaying the block delta).
	/// Both checkpoint directories are kept — the delta's memory chain
	/// resolves through the pre-copy base as a sibling — and only their
	/// peer-pullable CAS pointers are dropped.
	pub(crate) fn mesh_migrate_abort(
		&self,
		sid: &str,
		base_digest: &str,
		delta_dir: &Path,
		delta_digest: &str,
		mut params: Map<String, Value>,
	) -> Result<Value> {
		let rootfs = delta_dir.join("rootfs.img");
		if !rootfs.is_file() {
			let live = self
				.sandbox(sid)
				.rootfs_img()
				.ok()
				.filter(|path| path.is_file());
			let Some(live) = live else {
				return Err(EngineError::engine(format!(
					"cannot restore {sid}: its disk image is gone and the delta checkpoint {} carries \
					 no rootfs.img",
					delta_dir.display()
				)));
			};
			drop(vmm::create_cow_overlay(&live, &rootfs).map_err(|error| {
				EngineError::engine(format!(
					"adopting live rootfs {} -> {}: {error}",
					live.display(),
					rootfs.display()
				))
			})?);
		}
		if self.remove_local_candidate(sid).is_err() {
			// The re-create below must not self-conflict with a half-removed
			// record of the same sid.
			self.inner.registry.remove(sid);
		}
		params.insert("template".to_owned(), json!(delta_dir.to_string_lossy()));
		let view = self.mesh_create_from_params(params)?;
		image::cas::drop_pointer(delta_digest)?;
		image::cas::drop_pointer(base_digest)?;
		Ok(view)
	}

	/// Converge a durable target-side migration intent. Repeated calls are
	/// idempotent after a target has reached running; otherwise the staged
	/// final checkpoint is restored using the authoritative safe parameters.
	pub(crate) fn mesh_migrate_adopt_target(
		&self,
		sid: &str,
		delta_dir: &Path,
		mut params: Map<String, Value>,
	) -> Result<Value> {
		if let Some(record) = self.inner.registry.get(sid)
			&& matches!(record.status.as_str(), "running" | "paused")
		{
			return Ok(record.view());
		}
		params.insert("name".to_owned(), json!(sid));
		params.insert("template".to_owned(), json!(delta_dir.to_string_lossy()));
		let guard = self.inner.pending_migration_staging.lock().remove(sid);
		let result = self.mesh_create_from_params_paused(params);
		if let (Ok(_), Some(guard)) = (&result, guard) {
			self
				.inner
				.runtimes
				.lock()
				.entry(sid.to_owned())
				.or_default()
				.snapshot_source = Some(guard);
		}
		result
	}

	/// Retry only source-side post-commit cleanup. This never invokes the
	/// migration abort/restore path: target commitment remains authoritative.
	/// Materialize a verified migration delta outside CAS.  The returned path
	/// is retained by an engine-owned guard until the target runtime adopts it.
	pub(crate) fn mesh_stage_migration_delta(
		&self,
		sid: &str,
		verified_cache_path: &Path,
		base_path: &Path,
	) -> Result<PathBuf> {
		fn copy_tree(source: &Path, destination: &Path) -> Result<()> {
			copy_checkpoint_tree_cow(source, destination)
		}
		let runtime_root = self.inner.home.security_dir().join("runtime");
		fs::create_dir_all(&runtime_root)?;
		let base_name = base_path
			.file_name()
			.and_then(|name| name.to_str())
			.filter(|name| !name.is_empty() && *name != "." && *name != "..")
			.ok_or_else(|| EngineError::invalid("migration base has no valid directory name"))?;
		let delta_name = verified_cache_path
			.file_name()
			.and_then(|name| name.to_str())
			.filter(|name| !name.is_empty() && *name != "." && *name != "..")
			.ok_or_else(|| EngineError::invalid("migration delta has no valid directory name"))?;
		let stage_root = runtime_root.join(format!("migration-{}", uuid::Uuid::new_v4()));
		let stage_base = stage_root.join(base_name);
		let stage_delta = stage_root.join(delta_name);
		let staged = (|| -> Result<()> {
			fs::create_dir(&stage_root)?;
			copy_tree(base_path, &stage_base)?;
			copy_tree(verified_cache_path, &stage_delta)?;
			let snapshot = vmm::snapshot::read_state(&stage_delta).map_err(|error| {
				EngineError::invalid(format!("unreadable migration delta: {error}"))
			})?;
			let delta = snapshot
				.delta
				.ok_or_else(|| EngineError::invalid("migration checkpoint carries no memory delta"))?;
			if delta.base != base_name {
				return Err(EngineError::invalid("migration delta base does not match staged base"));
			}
			let rootfs = stage_delta.join("rootfs.img");
			let base_rootfs = stage_base.join("rootfs.img");
			if !base_rootfs.is_file() {
				return Err(EngineError::invalid("migration base carries no rootfs.img"));
			}
			drop(vmm::create_cow_overlay(&base_rootfs, &rootfs).map_err(|error| {
				EngineError::engine(format!(
					"materializing staged rootfs {} -> {}: {error}",
					base_rootfs.display(),
					rootfs.display()
				))
			})?);
			let disk_delta = stage_delta.join(diskdelta::DISK_DELTA_FILE);
			if disk_delta.is_file() {
				diskdelta::apply_disk_delta(&disk_delta, &rootfs)?;
				fs::remove_file(disk_delta)?;
			}
			fs::File::open(&rootfs)?.sync_all()?;
			Ok(())
		})();
		if let Err(error) = staged {
			let _ = fs::remove_dir_all(&stage_root);
			return Err(error);
		}
		self
			.inner
			.pending_migration_staging
			.lock()
			.insert(sid.to_owned(), Arc::new(TransientDir(stage_root)));
		Ok(stage_delta)
	}

	pub(crate) fn mesh_migrate_cleanup_committed(
		&self,
		sid: &str,
		base_dir: &Path,
		base_digest: &str,
		delta_dir: &Path,
		delta_digest: &str,
	) -> Result<()> {
		self.mesh_migrate_commit(sid, base_dir, base_digest, delta_dir, delta_digest)
	}

	/// Un-advertise an exact active export. Directory deletion is deferred to
	/// `ReplicaExport`'s last `Arc` drop so in-flight stream readers remain
	/// valid after publication cleanup.
	pub(crate) fn mesh_replicate_cleanup(&self, digest: &str, snapshot_dir: &Path) -> Result<()> {
		let mut exports = self.inner.pending_replica_exports.lock();
		let Some(sid) = exports.iter().find_map(|(sid, export)| {
			(export.digest == digest && export.path() == snapshot_dir).then(|| sid.clone())
		}) else {
			return Err(EngineError::busy("replica export is no longer the active exact checkpoint"));
		};
		self.mesh_drop_checkpoint(digest, snapshot_dir, false)?;
		drop(exports.remove(&sid));
		Ok(())
	}

	/// Bind the storage publication key to this exact in-memory export.
	pub(crate) fn mesh_bind_replica_export(
		&self,
		sid: &str,
		digest: &str,
		object_key: &str,
	) -> Result<()> {
		let export = self
			.inner
			.pending_replica_exports
			.lock()
			.get(sid)
			.cloned()
			.ok_or_else(|| EngineError::not_found("no active replica export for sandbox"))?;
		if export.digest != digest || !export.path().is_dir() {
			return Err(EngineError::busy("replica export does not match the requested digest"));
		}
		let mut bound = export.object_key.lock();
		match bound.as_deref() {
			None => *bound = Some(object_key.to_owned()),
			Some(existing) if existing == object_key => {},
			Some(_) => {
				return Err(EngineError::busy("replica export is already bound to another object"));
			},
		}
		Ok(())
	}

	/// Borrow the exact bound export for a peer stream. The returned `Arc`
	/// pins its directory independently of subsequent cleanup.
	pub(crate) fn mesh_replica_export(
		&self,
		sid: &str,
		digest: &str,
		object_key: &str,
	) -> Result<Arc<ReplicaExport>> {
		let export = self
			.inner
			.pending_replica_exports
			.lock()
			.get(sid)
			.cloned()
			.ok_or_else(|| EngineError::not_found("no active replica export for sandbox"))?;
		if export.digest != digest
			|| !export.path().is_dir()
			|| export.object_key.lock().as_deref() != Some(object_key)
		{
			return Err(EngineError::not_found("replica export does not match the requested object"));
		}
		Ok(export)
	}

	/// Un-advertise a checkpoint's CAS pointer; optionally delete its directory.
	#[allow(
		clippy::unused_self,
		reason = "checkpoint cleanup is kept beside related Engine mesh helpers"
	)]
	fn mesh_drop_checkpoint(
		&self,
		digest: &str,
		snapshot_dir: &Path,
		delete_dir: bool,
	) -> Result<()> {
		image::cas::drop_pointer(digest)?;
		if delete_dir && snapshot_dir.is_dir() {
			fs::remove_dir_all(snapshot_dir)?;
		}
		Ok(())
	}

	/// Materialize carried volumes, then create the sandbox from the
	/// checkpoint. Port of Python `Engine.restore_from_template`: validates the
	/// checkpoint's network flavor against this host, refuses volume-name
	/// collisions, and rolls back freshly-materialized volume directories if
	/// sandbox creation fails.
	pub(crate) fn mesh_restore_from_template(
		&self,
		params: Map<String, Value>,
		template_dir: &Path,
		_quorum_ok: bool,
	) -> Result<Value> {
		self.restore_from_template(params, template_dir, false)
	}

	fn restore_from_template(
		&self,
		mut params: Map<String, Value>,
		template_dir: &Path,
		replace_volumes: bool,
	) -> Result<Value> {
		validate_network_restore(&params)?;
		#[cfg(test)]
		let restore_executor = self.inner.restore_executor.lock().clone();
		#[cfg(test)]
		if let Some(executor) = restore_executor {
			let cancellation = params
				.get("name")
				.and_then(Value::as_str)
				.and_then(|id| self.launch_cancellation(id));
			return executor(self, params, template_dir, replace_volumes, cancellation);
		}
		let key_id = params
			.get("encryption_key_id")
			.and_then(Value::as_str)
			.unwrap_or("default");
		let mut backups = Vec::new();
		let created = if replace_volumes {
			backups = restore_checkpoint_volumes_in_place(
				self.home(),
				&self.inner.keyring,
				key_id,
				template_dir,
				params.get("volumes").and_then(Value::as_object),
			)?;
			Vec::new()
		} else {
			materialize_checkpoint_volumes(
				self.home(),
				&self.inner.keyring,
				key_id,
				template_dir,
				params.get("volumes").and_then(Value::as_object),
			)?
		};
		params.insert("template".to_owned(), json!(template_dir.to_string_lossy()));

		match self.mesh_create_from_params(params) {
			Ok(view) => {
				for backup in backups {
					backup.commit();
				}
				Ok(view)
			},
			Err(error) => {
				for path in created {
					remove_volume_artifact(&path);
				}
				for backup in backups.into_iter().rev() {
					backup.rollback()?;
				}

				Err(error)
			},
		}
	}

	/// Restore a portable checkpoint into a locally fenced, paused candidate.
	/// This is mesh-internal and deliberately bypasses public create JSON.
	pub(crate) fn mesh_restore_from_template_paused(
		&self,
		mut params: Map<String, Value>,
		template_dir: &Path,
		_quorum_ok: bool,
	) -> Result<Value> {
		validate_network_restore(&params)?;
		let key_id = params
			.get("encryption_key_id")
			.and_then(Value::as_str)
			.unwrap_or("default");
		let created = materialize_checkpoint_volumes(
			self.home(),
			&self.inner.keyring,
			key_id,
			template_dir,
			params.get("volumes").and_then(Value::as_object),
		)?;
		params.insert("template".to_owned(), json!(template_dir.to_string_lossy()));
		match self.mesh_create_from_params_paused(params) {
			Ok(view) => Ok(view),
			Err(error) => {
				for path in created {
					remove_volume_artifact(&path);
				}
				Err(error)
			},
		}
	}

	fn mesh_update_detail_fields(&self, sid: &str, fields: Map<String, Value>) -> Result<()> {
		self
			.inner
			.registry
			.update_detail_persisted(self.home(), sid, move |detail| {
				detail.extend(fields);
			})?;
		Ok(())
	}

	/// Test constructor: aligns `VMON_HOME` with the configured home under the
	/// process-wide test lock and keeps it held for the returned guard's
	/// lifetime, so env-resolved helpers (CAS, `MicroVm` paths) stay isolated
	/// per test.
	#[cfg(test)]
	fn new_test(config: ServeConfig) -> (Self, crate::home::test_home::HomeGuard) {
		let guard = crate::home::test_home::set(&config.home);
		(Self::new(config).expect("test engine constructs"), guard)
	}

	#[cfg(test)]
	fn insert_test_record(&self, record: VmRecord) {
		self.inner.registry.insert(record);
	}

	#[cfg(test)]
	fn test_capture_lock_count(&self) -> usize {
		self.inner.capture_locks.lock().len()
	}

	#[cfg(test)]
	pub(crate) fn test_set_restore_executor(&self, executor: Arc<TestRestoreExecutor>) {
		*self.inner.restore_executor.lock() = Some(executor);
	}

	#[cfg(test)]
	pub(crate) fn test_set_capture_executor(&self, executor: Arc<TestCaptureExecutor>) {
		*self.inner.capture_executor.lock() = Some(executor);
	}

	#[cfg(test)]
	pub(crate) fn test_set_rollback_resume_executor(
		&self,
		executor: Arc<TestRollbackResumeExecutor>,
	) {
		*self.inner.rollback_resume_executor.lock() = Some(executor);
	}

	fn rollback_resume_guard(&self, id: &str) -> ResumeOnError {
		#[cfg(test)]
		{
			return ResumeOnError::with_resume_executor(
				self.sandbox(id),
				self.inner.rollback_resume_executor.lock().clone(),
			);
		}
		#[cfg(not(test))]
		ResumeOnError::new(self.sandbox(id))
	}

	#[cfg(test)]
	pub(crate) fn test_clear_restore_executor(&self) {
		*self.inner.restore_executor.lock() = None;
	}

	#[cfg(test)]
	pub(crate) fn test_restore_cancellation_token(&self, id: &str) -> Arc<AtomicBool> {
		self
			.inner
			.launch_cancellations
			.lock()
			.entry(id.to_owned())
			.or_insert_with(|| Arc::new(AtomicBool::new(false)))
			.clone()
	}

	#[cfg(test)]
	pub(crate) fn test_clear_restore_cancellation(&self, id: &str) {
		self.inner.launch_cancellations.lock().remove(id);
	}

	#[cfg(test)]
	pub(crate) fn test_recover_rollback_journals_after_durable(&self) -> Result<()> {
		self.recover_rollback_journals()
	}

	/// Drives the crash-sensitive half of suspended-state reconciliation with
	/// injected control callbacks. This keeps the production ordering (resume
	/// before the generation-fenced cancellation) directly testable without a
	/// VMM control socket.
	#[cfg(test)]
	pub(crate) fn test_resume_unpublished_suspend<F, G>(
		&self,
		id: &str,
		resume: F,
		after_resume: G,
	) -> Result<()>
	where
		F: FnOnce() -> Result<()>,
		G: FnOnce() -> Result<()>,
	{
		let record = self.get_record(id, false)?;
		let lifecycle = record.lifecycle.clone();
		if lifecycle.desired != LifecyclePhase::Suspended
			|| record
				.detail
				.get("suspend_recovery_point")
				.and_then(Value::as_str)
				.is_some()
		{
			return Err(EngineError::invalid(
				"test record is not an unpublished suspended transition",
			));
		}
		resume()?;
		after_resume()?;
		self.inner.registry.cancel_transition(
			self.home(),
			id,
			lifecycle.generation,
			LifecyclePhase::Running,
			"suspend publication was interrupted before its recovery point committed",
		)?;
		Ok(())
	}
}

impl Engine {
	fn stop_locked(&self, id: &str, returncode: Option<i64>) -> Result<Value> {
		let record = self.get_record(id, false)?;
		if record.status == "terminated" {
			return Ok(json!({ "name": id, "status": record.status }));
		}
		if record.status == "stopped" {
			self.apply_ephemeral_discard(&record)?;
			return Ok(json!({ "name": id, "status": record.status }));
		}
		let agent = self
			.inner
			.runtimes
			.lock()
			.get(&record.name)
			.and_then(|runtime| runtime.agent.clone())
			.ok_or_else(|| {
				EngineError::busy(format!(
					"sandbox '{id}' cannot stop durably while its guest agent is unavailable"
				))
			})?;
		let was_paused = record.lifecycle.observed == LifecyclePhase::Paused;
		let flush_result = (|| -> Result<()> {
			if was_paused {
				control_for_vm(&self.sandbox(&record.name))?.resume()?;
				agent.ping(AGENT_REQUEST_TIMEOUT)?;
			}
			agent.fs_sync(AGENT_REQUEST_TIMEOUT)?;
			Ok(())
		})();
		if let Err(error) = flush_result {
			if was_paused {
				let _ =
					control_for_vm(&self.sandbox(&record.name)).and_then(|mut control| control.pause());
			}
			return Err(error);
		}
		let transition = self.begin_state_transition(id, LifecyclePhase::Stopped)?;
		if transition.disposition != TransitionDisposition::Acquired {
			return Ok(self.get_record(id, false)?.view());
		}
		let generation = transition.generation;
		let teardown_rc = match self.teardown(&record) {
			Ok(code) => code,
			Err(error) => {
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			},
		};
		if let Err(error) = self.persist_status(id, "stopped", returncode.or(teardown_rc), None) {
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		if let Err(error) = self.apply_ephemeral_discard(&record) {
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		self.complete_state_transition(id, generation, LifecyclePhase::Stopped)?;
		let record = self.inner.registry.get(id).unwrap_or(record);
		self.publish_record_event("stopped", &record);
		Ok(json!({ "name": id, "status": "stopped" }))
	}

	fn record_used_process_secrets(record: &VmRecord) -> bool {
		record
			.detail
			.get("cold_start_requires_process_secrets")
			.and_then(Value::as_bool)
			.unwrap_or_else(|| {
				record
					.detail
					.get("secret_names")
					.and_then(Value::as_array)
					.is_some_and(|names| !names.is_empty())
					|| record
						.detail
						.get("credential_names")
						.and_then(Value::as_array)
						.is_some_and(|names| !names.is_empty())
			})
	}

	fn recipe_for_cold_start(&self, record: &VmRecord) -> Result<RelaunchRecipe> {
		let recipe = self.inner.relaunch_recipes.lock().get(&record.id).cloned();
		if let Some(recipe) = recipe {
			return Ok(recipe);
		}
		if Self::record_used_process_secrets(record) {
			return Err(EngineError::busy(
				"cold start unavailable: sandbox used secrets and the server restarted; recreate or \
				 restore from snapshot",
			));
		}
		let persisted = record
			.detail
			.get("relaunch_recipe")
			.and_then(Value::as_object)
			.ok_or_else(|| {
				EngineError::busy(
					"cold start unavailable: relaunch metadata is incomplete; recreate or restore from \
					 snapshot",
				)
			})?;
		let params = serde_json::from_value(
			persisted
				.get("params")
				.cloned()
				.ok_or_else(|| EngineError::busy("cold start relaunch parameters are missing"))?,
		)
		.map_err(|_| EngineError::busy("cold start relaunch parameters are invalid"))?;
		let template_dir = persisted
			.get("template_dir")
			.and_then(Value::as_str)
			.map(PathBuf::from)
			.ok_or_else(|| EngineError::busy("cold start template metadata is missing"))?;
		let image_ref = persisted
			.get("image_ref")
			.and_then(Value::as_str)
			.map(str::to_owned);
		Ok(RelaunchRecipe { params, template_dir, image_spec: None, image_ref })
	}

	fn cold_start(&self, id: &str) -> Result<Value> {
		let record = self
			.inner
			.registry
			.get(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		if !self.inner.relaunch_recipes.lock().contains_key(id)
			&& Self::record_used_process_secrets(&record)
		{
			return Err(EngineError::busy(
				"cold start unavailable: sandbox used secrets and the server restarted; recreate or \
				 restore from snapshot",
			));
		}
		let vm = self.sandbox(&record.name);
		if !vm.dir().is_dir() || !vm.dir().join("rootfs.img").is_file() {
			return Err(EngineError::not_found(format!("sandbox '{id}' has no retained rootfs.img")));
		}
		let recipe = self.recipe_for_cold_start(&record)?;
		let mut plan = self.prepare_relaunch(&recipe)?;
		let transition = self.begin_state_transition(id, LifecyclePhase::Running)?;
		if transition.disposition != TransitionDisposition::Acquired {
			return Ok(self.get_record(id, false)?.view());
		}
		let generation = transition.generation;
		let (vm, runtime) = match self.launch_create(&mut plan, false) {
			Ok(launched) => launched,
			Err(error) => {
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			},
		};
		if let Err(error) = self.persist_status(id, "running", None, None) {
			let mut runtime = runtime;
			self.rollback_uncommitted_runtime(&vm, &mut runtime, true);
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		self.inner.runtimes.lock().insert(record.name, runtime);
		if let Ok(meta) = vm.meta() {
			let pid = meta
				.get("pid")
				.and_then(Value::as_i64)
				.and_then(|pid| i32::try_from(pid).ok());
			let _ = self.inner.registry.update(id, |record| record.pid = pid);
		}
		if let Err(error) = self.complete_state_transition(id, generation, LifecyclePhase::Running) {
			if let Some(record) = self.inner.registry.get(id) {
				let _ = self.teardown(&record);
				let _ = self.persist_status(id, "stopped", None, None);
			}
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		let record = self.get_record(id, false)?;
		self.publish_record_event("resumed", &record);
		Ok(record.view())
	}

	#[allow(clippy::unused_self, reason = "uses self under test builds for mock executor")]
	fn ensure_disk_resize_available(&self) -> Result<()> {
		#[cfg(test)]
		if self.inner.disk_resize_executor.lock().is_some() {
			return Ok(());
		}
		for tool in ["e2fsck", "resize2fs"] {
			if Command::new(tool).arg("-V").output().is_err() {
				return Err(EngineError::invalid("disk resize requires host e2fsprogs"));
			}
		}
		Ok(())
	}

	#[allow(clippy::unused_self, reason = "uses self under test builds for mock executor")]
	fn build_resized_disk(&self, source: &Path, stage: &Path, new_bytes: u64) -> Result<()> {
		#[cfg(test)]
		let executor = self.inner.disk_resize_executor.lock().clone();
		#[cfg(test)]
		if let Some(executor) = executor {
			return executor(source, stage, new_bytes);
		}
		let _ = fs::remove_file(stage);
		let reflinked = Command::new("cp")
			.args(["--reflink=auto", "--sparse=always"])
			.arg(source)
			.arg(stage)
			.status()
			.is_ok_and(|status| status.success());
		if !reflinked {
			fs::copy(source, stage).map_err(|error| {
				EngineError::invalid(format!("staging disk resize failed: {error}"))
			})?;
		}
		OpenOptions::new()
			.write(true)
			.open(stage)
			.and_then(|file| file.set_len(new_bytes))
			.map_err(|error| EngineError::invalid(format!("growing staged disk failed: {error}")))?;
		for (tool, args, accept_repaired) in [
			("e2fsck", vec!["-pf"], true),
			("resize2fs", Vec::new(), false),
			("e2fsck", vec!["-pf"], true),
		] {
			let output = Command::new(tool)
				.args(args)
				.arg(stage)
				.output()
				.map_err(|_| EngineError::invalid("disk resize requires host e2fsprogs"))?;
			let code = output.status.code();
			if !(output.status.success() || (accept_repaired && code == Some(1))) {
				let stderr = String::from_utf8_lossy(&output.stderr);
				let stdout = String::from_utf8_lossy(&output.stdout);
				let detail = if stderr.trim().is_empty() {
					stdout.trim()
				} else {
					stderr.trim()
				};
				return Err(EngineError::invalid(format!(
					"{tool} failed during disk resize: {detail}"
				)));
			}
		}
		Ok(())
	}
}

impl TemplateBooter for Engine {
	fn boot_verify_and_snapshot(&self, spec: &TemplateSpec) -> Result<()> {
		let old = self.sandbox(&spec.vm_name);
		if self.sandbox_is_running(&old).unwrap_or(false) {
			let _ = self.stop_sandbox(&old, true);
		}
		let _ = self.remove_sandbox(&old);
		let (tap_name, guest_config) = if spec.tap_slot {
			let config = net::allocate_guest_config(&spec.vm_name)?;
			net::setup_tap(
				&config.tap,
				&config.guest_ip,
				&config.host_ip,
				config.prefix,
				None,
				None,
				None,
			)?;
			(Some(config.tap.clone()), Some(config))
		} else {
			(None, None)
		};
		let vm = self.sandbox(&spec.vm_name);
		let launch_result = (|| -> Result<()> {
			let mut launch = LaunchSpec::boot_rootfs(
				vm.api_sock(),
				image::assets::default_kernel()?,
				&spec.rootfs_ext4,
			)
			.with_agent_sock(vm.dir().join("agent.sock"))
			.with_mem_mib(spec.memory)
			.with_cpus(spec.cpus)
			.with_rng()
			.with_snapshot_root(&spec.snapshot_root);
			if spec.user_net {
				launch = launch.with_user_net();
			}
			if let Some(tap) = &tap_name {
				launch = launch.with_tap(tap.clone());
			}
			if let Some(fs_dir) = &spec.fs_dir {
				launch = launch.with_fs_share("host", fs_dir);
			}
			for volume in &spec.volumes {
				launch =
					launch.with_volume(VolumeMount::new(&volume.tag, &volume.dir, volume.readonly)?);
			}
			self.launch_sandbox(&vm, &launch)?;
			Self::agent_for_vm(&vm, Duration::from_secs(spec.timeout))?
				.ping(Duration::from_secs(spec.timeout))?;
			self.snapshot_machine(
				&vm,
				&spec.template_name,
				false,
				Some(&spec.rootfs_ext4),
				&spec.snapshot_root,
				false,
			)?;
			Ok(())
		})();
		let _ = self.remove_sandbox(&vm);
		if let Some(config) = guest_config {
			let _ = net::teardown_tap(
				&config.tap,
				Some(&config.guest_ip),
				Some(&config.host_ip),
				config.prefix,
				None,
				None,
			);
			let _ = net::release_guest_config(&spec.vm_name);
		}
		launch_result
	}
}

impl EngineApi for Engine {
	fn adopt_suspended_marker(&self, id: &str) -> Result<()> {
		self.mesh_adopt_suspended_marker(id)
	}

	fn create(&self, params: SandboxCreate) -> Result<Value> {
		if let Some(key) = params
			.idempotency_key
			.as_deref()
			.filter(|key| !key.is_empty())
			&& let Some(name) = self.inner.registry.find_by_idempotency_key(key)
			&& let Some(record) = self.inner.registry.get(&name)
		{
			if record.status != "terminated" {
				return Ok(record.view());
			}
			self.inner.registry.remove_idempotency_for(key, &name);
		}
		let requested_sid = params.name.clone().filter(|name| !name.is_empty());
		let request_time = Instant::now();
		let mut plan = match self.prepare_create(params) {
			Ok(plan) => plan,
			Err(error) => {
				if let Some(sid) = requested_sid {
					self.publish_create_failure(&sid, &error);
				}
				return Err(error);
			},
		};
		self.publish_event(
			"creating",
			Map::from_iter([
				("id".to_owned(), json!(plan.sid)),
				("name".to_owned(), json!(plan.sid)),
				("status".to_owned(), json!("creating")),
				("tags".to_owned(), json!(plan.tags)),
			]),
		);
		let (vm, runtime) = match self.launch_create(&mut plan, false) {
			Ok(result) => result,
			Err(err) => {
				self.publish_create_failure(&plan.sid, &err);
				drop(plan);
				return Err(err);
			},
		};
		let now = unix_time();
		let meta = vm.meta()?;
		let mut detail = meta.clone();
		detail.insert("image".to_owned(), json!(plan.params.image));
		detail.insert(
			"template".to_owned(),
			json!(
				plan
					.params
					.template
					.clone()
					.unwrap_or_else(|| plan.template_dir.to_string_lossy().into_owned())
			),
		);
		detail.insert("tags".to_owned(), json!(plan.tags));
		detail.insert("cpus".to_owned(), json!(plan.params.cpus));
		detail.insert("memory".to_owned(), json!(plan.params.memory));
		detail.insert("disk_mb".to_owned(), json!(plan.params.disk_mb));
		detail.insert("block_network".to_owned(), json!(plan.params.block_network));
		detail.insert("egress_allow".to_owned(), json!(plan.params.egress_allow));
		detail.insert("egress_allow_domains".to_owned(), json!(plan.params.egress_allow_domains));
		detail.insert("inbound_cidr_allowlist".to_owned(), json!(plan.params.inbound_cidr_allowlist));
		detail.insert("nics".to_owned(), json!(plan.params.nics));
		detail.insert("pool_size".to_owned(), json!(plan.params.pool_size));
		if let Some(fs_dir) = &plan.params.fs_dir {
			// Host-local share: checkpointing/migration must refuse this sandbox.
			detail.insert("fs_dir".to_owned(), json!(fs_dir));
		}
		detail.insert("volumes".to_owned(), volumes_meta(&plan.volume_specs));
		detail.insert("s3_mounts".to_owned(), s3_mounts_meta(&plan.s3_specs));
		detail.insert(
			"create_latency_ms".to_owned(),
			json!(request_time.elapsed().as_secs_f64() * 1000.0),
		);
		detail.insert("ha".to_owned(), json!(plan.ha));
		detail.insert("restart_policy".to_owned(), json!(plan.restart_policy));
		let requires_process_secrets = !plan.secrets.is_empty()
			|| plan
				.relaunch_params
				.credentials
				.as_ref()
				.is_some_and(|names| !names.is_empty())
			|| !plan.s3_specs.is_empty();
		detail
			.insert("cold_start_requires_process_secrets".to_owned(), json!(requires_process_secrets));
		let mut recipe_params = plan.relaunch_params.clone();
		recipe_params.env = Some(runtime.env.clone().into_iter().collect());
		recipe_params.workdir.clone_from(&runtime.workdir);
		let recipe = RelaunchRecipe {
			params:       recipe_params,
			template_dir: plan.template_dir.clone(),
			image_spec:   plan.image_spec.clone(),
			image_ref:    plan.image_ref.clone(),
		};
		if !requires_process_secrets {
			let mut persisted_params = recipe.params.clone();
			persisted_params.secrets = None;
			detail.insert(
				"relaunch_recipe".to_owned(),
				json!({
					"params": persisted_params,
					"template_dir": recipe.template_dir,
					"image_ref": recipe.image_ref,
				}),
			);
		}
		if let Some(command) = &plan.params.command {
			detail.insert("command".to_owned(), json!(command));
		}
		if let Some(key) = plan
			.params
			.idempotency_key
			.as_deref()
			.filter(|key| !key.is_empty())
		{
			detail.insert("idempotency_key".to_owned(), json!(key));
			let mut update = Map::new();
			update.insert("idempotency_key".to_owned(), json!(key));
			let _ = vm.save_meta(update);
		}
		let runtime_identity = safe_runtime_identity(
			&runtime,
			plan.secrets.iter().map(|secret| secret.name.clone()),
			plan.timeout_secs.map(|secs| secs as f64),
			plan
				.params
				.image
				.clone()
				.or_else(|| Some(plan.template_dir.to_string_lossy().into_owned())),
			Some(plan.template_dir.to_string_lossy().into_owned()),
		);
		let record = VmRecord {
			id: plan.sid.clone(),
			name: plan.sid.clone(),
			status: "running".to_owned(),
			pid: meta
				.get("pid")
				.and_then(Value::as_i64)
				.and_then(|pid| i32::try_from(pid).ok()),
			source: plan
				.params
				.image
				.clone()
				.or_else(|| Some(plan.template_dir.to_string_lossy().into_owned())),
			incarnation_epoch: detail
				.get("incarnation_epoch")
				.and_then(Value::as_i64)
				.unwrap_or(0),
			created_at: now,
			timeout: plan.timeout_secs.map(|secs| secs as f64),
			detail: Value::Object(detail),
			tags: plan.tags.clone(),
			last_active: now,
			last_network_active: now,
			persistence: plan.params.persistence.clone().unwrap_or_default(),
			terminated_at: None,
			error: None,
			lifecycle: LifecycleState {
				desired:    LifecyclePhase::Running,
				observed:   LifecyclePhase::Running,
				generation: StateGeneration(1),
				failure:    None,
				operation:  None,
			},
			runtime_identity,
		};
		if let Err(error) = self
			.inner
			.registry
			.insert_persisted(self.home(), record.clone())
		{
			let mut runtime = runtime;
			self.rollback_uncommitted_runtime(&vm, &mut runtime, false);
			return Err(error);
		}
		self.inner.runtimes.lock().insert(plan.sid.clone(), runtime);
		self.wake_maintenance();
		self
			.inner
			.relaunch_recipes
			.lock()
			.insert(plan.sid.clone(), recipe);
		if let Some(key) = plan
			.params
			.idempotency_key
			.as_deref()
			.filter(|key| !key.is_empty())
		{
			self.inner.registry.record_idempotency(&plan.sid, key);
		}
		self.inc_counter("created");
		{
			let mut latency = self.inner.latency.lock();
			latency.sum_ms = request_time
				.elapsed()
				.as_secs_f64()
				.mul_add(1000.0, latency.sum_ms);
			latency.count += 1;
		}
		self.publish_record_event("created", &record);
		self.publish_record_event("ready", &record);
		if let Some(command) = plan
			.params
			.command
			.clone()
			.filter(|command| !command.is_empty())
		{
			self.start_entry_command(plan.sid.clone(), command)?;
		}
		Ok(record.view())
	}

	fn list(&self, tags: Option<HashMap<String, String>>) -> Result<Vec<Value>> {
		let records = self.inner.registry.list();
		Ok(records
			.into_iter()
			// Refresh liveness so self-terminated VMs (timeout, poweroff) list
			// with their final status, mirroring Python's ps() refresh.
			.map(|record| self.refresh_record_status(record.clone()).unwrap_or(record))
			.filter(|record| {
				let Some(tags) = &tags else {
					return true;
				};
				let available = record
					.detail
					.get("tags")
					.and_then(Value::as_object)
					.map(|object| {
						object
							.iter()
							.filter_map(|(key, value)| value.as_str().map(|value| (key.as_str(), value)))
							.collect::<HashMap<_, _>>()
					})
					.unwrap_or_default();
				tags.iter().all(|(key, value)| {
					available
						.get(key.as_str())
						.is_some_and(|found| found == value)
				})
			})
			.map(|record| record.view())
			.collect())
	}

	fn orchestration_inventory(&self) -> Result<Vec<Value>> {
		Ok(self
			.inner
			.registry
			.list()
			.into_iter()
			.map(|record| record.view())
			.collect())
	}

	fn get(&self, id: &str) -> Result<Value> {
		Ok(self.get_record(id, false)?.view())
	}

	fn stop(&self, id: &str) -> Result<Value> {
		self.stop_with_returncode(id, None)
	}

	fn stop_with_returncode(&self, id: &str, returncode: Option<i64>) -> Result<Value> {
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		self.stop_locked(id, returncode)
	}

	fn terminate(&self, id: &str, reason: &str) -> Result<Value> {
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		let record = self.get_record(id, true)?;
		let returncode = self.teardown(&record)?;
		let terminated_at = unix_time();
		self.persist_status(id, "terminated", returncode, Some(terminated_at))?;
		self.inner.vpcs.release_sandbox(id)?;
		let oom = detect_oom(id);
		let actual_reason = if oom { "oom" } else { reason };
		let _ = self
			.inner
			.registry
			.update_detail_persisted(self.home(), id, |detail| {
				detail.insert("terminated_reason".to_owned(), json!(actual_reason));
				detail.insert("oom".to_owned(), json!(oom));
			});
		let record = self
			.inner
			.registry
			.get(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		self.inc_counter("terminated");
		self.publish_record_event("terminated", &record);
		Ok(record.view())
	}

	fn remove(&self, id: &str) -> Result<Value> {
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		let record = self.get_record(id, false)?;
		let delete = self.lifecycle_handoff_owner(id)?;
		if let Some(delete) = &delete {
			delete
				.handoff
				.begin_delete(id, &delete.owner, delete.epoch)?;
		}
		self.teardown(&record)?;
		let vm = self.sandbox(&record.name);
		if let Some(delete) = delete {
			Self::commit_portable_delete_after_history(
				|| {
					self.delete_portable_history(id, &delete.owner, delete.epoch)?;
					self.delete_local_recovery_history(id)?;
					self.remove_sandbox(&vm)
				},
				|| {
					delete
						.handoff
						.commit_delete(id, &delete.owner, delete.epoch)
				},
			)?;
		} else {
			self.delete_local_recovery_history(id)?;
			self.remove_sandbox(&vm)?;
		}
		self.inner.vpcs.release_sandbox(id)?;
		self.inner.registry.remove(id);
		self.inner.relaunch_recipes.lock().remove(id);
		Ok(json!({ "name": id, "removed": true }))
	}

	fn pause(&self, id: &str) -> Result<Value> {
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		let transition = self.begin_state_transition(id, LifecyclePhase::Paused)?;
		if transition.disposition != TransitionDisposition::Acquired {
			return Ok(self.get_record(id, false)?.view());
		}
		let generation = transition.generation;
		let result = match control_for_vm(&self.sandbox(id)).and_then(|mut control| control.pause()) {
			Ok(result) => result,
			Err(error) => {
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			},
		};
		if let Err(error) = self.complete_state_transition(id, generation, LifecyclePhase::Paused) {
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		let record = self
			.inner
			.registry
			.get(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		self.publish_record_event("paused", &record);
		Ok(result)
	}

	fn resume(&self, id: &str) -> Result<Value> {
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		let record = self.get_record(id, false)?;
		if record.status == "stopped" {
			return self.cold_start(id);
		}
		if self.converge_pending_portable_resume(&record)? {
			let record = self.get_record(id, false)?;
			self.publish_record_event("resumed", &record);
			return Ok(record.view());
		}
		// A response may be lost after the database atomically commits the
		// replacement. Resume converges that exact already-running candidate;
		// it never claims a new epoch or relaunches it.
		if record.status == "running" {
			let portable = {
				let mut ownership = self.inner.portable_ownership.lock();
				if ownership.is_none() {
					*ownership = PortableOwnership::connect(&self.inner.config)?;
				}
				ownership.as_ref().cloned()
			};
			if let Some(portable) = portable
				&& let Some(marker) = portable.suspend_marker(id)?
				&& marker.state == "resuming"
			{
				if record.lifecycle.desired != LifecyclePhase::Running
					|| record.lifecycle.observed == LifecyclePhase::Running
					|| record.detail.get("recovery_point").and_then(Value::as_str)
						!= Some(marker.point.as_str())
				{
					return Err(EngineError::busy(
						"resuming marker does not match the retained local replacement",
					));
				}
				let handoff = self
					.inner
					.restore_handoff
					.lock()
					.as_ref()
					.and_then(Weak::upgrade)
					.ok_or_else(|| {
						EngineError::busy("resuming marker lacks a local ownership handoff")
					})?;
				let lease = portable.lease_for_resuming_marker(&marker)?.lease;
				if let Err(error) = handoff.commit_restore(id, &lease.owner_node, lease.epoch)
					&& handoff
						.commit_restore(id, &lease.owner_node, lease.epoch)
						.is_err()
				{
					return Err(error);
				}
				self.disarm_restore_volume_leases(id);
				self.complete_state_transition(
					id,
					record.lifecycle.generation,
					LifecyclePhase::Running,
				)?;
				let record = self.get_record(id, false)?;
				self.publish_record_event("resumed", &record);
				return Ok(record.view());
			}
		}
		if record.status == "suspended" {
			// Production suspension is recovered only from the cluster marker:
			// local detail is a placeholder and must never choose a newer
			// history point after ownership moved to another node.
			let portable = {
				let mut ownership = self.inner.portable_ownership.lock();
				if ownership.is_none() {
					*ownership = PortableOwnership::connect(&self.inner.config)?;
				}
				ownership.as_ref().cloned()
			};
			let Some(portable) = portable else {
				let recovery_point = record
					.detail
					.get("suspend_recovery_point")
					.and_then(Value::as_str)
					.ok_or_else(|| EngineError::not_found("suspended sandbox has no recovery point"))?;
				let transition = self.begin_state_transition(id, LifecyclePhase::Running)?;
				if transition.disposition != TransitionDisposition::Acquired {
					return Ok(self.get_record(id, false)?.view());
				}
				let generation = transition.generation;
				let view = self
					.restore_recovery_identity(id, recovery_point, None)
					.inspect_err(|error| {
						self.fail_state_transition(id, generation, error);
					})?;
				self.complete_state_transition(id, generation, LifecyclePhase::Running)?;
				if let Some(record) = self.inner.registry.get(id) {
					self.publish_record_event("resumed", &record);
				}
				return Ok(view);
			};
			let marker = portable.suspend_marker(id)?.ok_or_else(|| {
				EngineError::not_found("suspended sandbox has no durable recovery point")
			})?;
			let continuing_adopted_resume = marker.state == "resuming";
			if marker.state != "suspended" && !continuing_adopted_resume {
				return Err(EngineError::busy(format!("suspended sandbox '{id}' is {}", marker.state)));
			}
			let handoff = Some(
				self
					.inner
					.restore_handoff
					.lock()
					.as_ref()
					.and_then(Weak::upgrade)
					.ok_or_else(|| {
						EngineError::engine("portable resume requires an ownership handoff")
					})?,
			);
			if continuing_adopted_resume {
				let detail = record.detail.as_object().ok_or_else(|| {
					EngineError::busy("adopted suspended projection has invalid metadata")
				})?;
				if detail
					.get("_mesh_suspended_projection")
					.and_then(Value::as_bool)
					!= Some(true)
					|| detail.get("suspend_recovery_point").and_then(Value::as_str)
						!= Some(marker.point.as_str())
					|| detail.get("suspend_generation").and_then(Value::as_u64)
						!= Some(marker.generation)
					|| detail.get("suspend_owner").and_then(Value::as_str) != Some(marker.owner.as_str())
					|| detail.get("suspend_owner_epoch").and_then(Value::as_i64) != Some(marker.epoch)
				{
					return Err(EngineError::busy(
						"adopted resuming marker does not match local projection",
					));
				}
			}
			let transition = self.begin_state_transition(id, LifecyclePhase::Running)?;
			if transition.disposition != TransitionDisposition::Acquired {
				return Ok(self.get_record(id, false)?.view());
			}
			let generation = transition.generation;
			let lease = if continuing_adopted_resume {
				match portable.lease_for_resuming_marker(&marker) {
					Ok(claimed) => claimed.lease,
					Err(error) => {
						self.fail_state_transition(id, generation, &error);
						return Err(error);
					},
				}
			} else {
				match portable.claim_expected(id, &marker.owner, marker.epoch, marker.generation) {
					Ok(lease) => lease,
					Err(error) => {
						self.fail_state_transition(id, generation, &error);
						return Err(error);
					},
				}
			};
			if !continuing_adopted_resume {
				let verified = match portable.verify(id, &lease.owner_node, lease.epoch) {
					Ok(verified) => verified,
					Err(error) => {
						let _ = portable.abort(&lease);
						self.fail_state_transition(id, generation, &error);
						return Err(error);
					},
				};
				if !verified {
					let _ = portable.abort(&lease);
					let error = EngineError::busy("suspended resume ownership was superseded");
					self.fail_state_transition(id, generation, &error);
					return Err(error);
				}
				if let Some(handoff) = &handoff
					&& let Err(error) = handoff.begin_restore(id, &lease.owner_node, lease.epoch)
				{
					let _ = portable.abort(&lease);
					self.fail_state_transition(id, generation, &error);
					return Err(error);
				}
			}
			let heartbeat = match portable.start_restore_heartbeat(&lease) {
				Ok(heartbeat) => heartbeat,
				Err(error) => {
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let _ = portable.abort(&lease);
					self.fail_state_transition(id, generation, &error);
					return Err(error);
				},
			};
			self
				.inner
				.launch_cancellations
				.lock()
				.insert(id.to_owned(), heartbeat.lost_signal());
			let restore_engine = self.clone();
			let restore_id = id.to_owned();
			let recovery_point = marker.point;
			let restore_lease = handoff
				.as_ref()
				.map(|handoff| (Arc::clone(handoff), lease.owner_node.clone(), lease.epoch));
			let launch = match thread::Builder::new()
				.name(format!("suspended-resume-{id}"))
				.spawn(move || {
					restore_engine.restore_recovery_identity(&restore_id, &recovery_point, restore_lease)
				}) {
				Ok(launch) => launch,
				Err(error) => {
					self.inner.launch_cancellations.lock().remove(id);
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let _ = portable.abort(&lease);
					let error = EngineError::engine(format!("starting suspended resume: {error}"));
					self.fail_state_transition(id, generation, &error);
					return Err(error);
				},
			};
			let Ok(launch_result) = launch.join() else {
				self.inner.launch_cancellations.lock().remove(id);
				self.remove_restore_candidate(id)?;
				if let Some(handoff) = &handoff {
					let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
				}
				let _ = portable.abort(&lease);
				let error = EngineError::engine("suspended resume worker panicked");
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			};
			self.inner.launch_cancellations.lock().remove(id);
			let authorized = match self.inner.net_runtime.block_on(heartbeat.finish()) {
				Ok(authorized) => authorized,
				Err(error) => {
					if launch_result.is_ok() {
						self.remove_restore_candidate(id)?;
					}
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let _ = portable.abort(&lease);
					self.fail_state_transition(id, generation, &error);
					return Err(error);
				},
			};
			let view = match launch_result {
				Ok(view) if authorized => view,
				Ok(_) => {
					self.remove_restore_candidate(id)?;
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let _ = portable.abort(&lease);
					let error = EngineError::busy("suspended resume lease was lost during restore");
					self.fail_state_transition(id, generation, &error);
					return Err(error);
				},
				Err(error) => {
					self.remove_restore_candidate(id)?;
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let _ = portable.abort(&lease);
					self.fail_state_transition(id, generation, &error);
					return Err(error);
				},
			};
			if let Err(error) = portable.finalize(&lease) {
				self.remove_restore_candidate(id)?;
				if let Some(handoff) = &handoff {
					let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
				}
				let _ = portable.abort(&lease);
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			}
			if let Some(handoff) = &handoff
				&& let Err(error) = handoff.commit_restore(id, &lease.owner_node, lease.epoch)
				&& handoff
					.commit_restore(id, &lease.owner_node, lease.epoch)
					.is_err()
			{
				// The commit can have won before its response was lost.
				// Do not release fresh votes or resume/abort the marker.
				return Err(error);
			}
			self.disarm_restore_volume_leases(id);
			self.complete_state_transition(id, generation, LifecyclePhase::Running)?;
			if let Some(record) = self.inner.registry.get(id) {
				self.publish_record_event("resumed", &record);
			}
			return Ok(view);
		}
		if record.status != "running" && record.status != "paused" {
			return Err(EngineError::not_running(format!("sandbox '{id}' is not running")));
		}
		let transition = self.begin_state_transition(id, LifecyclePhase::Running)?;
		if transition.disposition != TransitionDisposition::Acquired {
			return Ok(self.get_record(id, false)?.view());
		}
		let generation = transition.generation;
		let result = match control_for_vm(&self.sandbox(id)).and_then(|mut control| control.resume())
		{
			Ok(result) => result,
			Err(error) => {
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			},
		};
		self.complete_state_transition(id, generation, LifecyclePhase::Running)?;
		if let Some(record) = self.inner.registry.get(id) {
			self.publish_record_event("resumed", &record);
		}
		Ok(result)
	}

	fn resize(
		&self,
		id: &str,
		cpus: Option<u32>,
		memory_mib: Option<u32>,
		disk_mb: Option<u64>,
	) -> Result<Value> {
		if cpus.is_none() && memory_mib.is_none() && disk_mb.is_none() {
			return Err(EngineError::invalid("resize requires at least one resource field"));
		}
		if cpus == Some(0) || memory_mib == Some(0) || disk_mb == Some(0) {
			return Err(EngineError::invalid("resize resource sizes must be nonzero"));
		}
		if cpus.is_some_and(|value| u64::from(value) > MAX_CPUS) {
			return Err(EngineError::invalid(format!("cpus must be at most {MAX_CPUS}")));
		}
		if memory_mib.is_some_and(|value| u64::from(value) > MAX_MEM_MIB) {
			return Err(EngineError::invalid(format!("memory_mib must be at most {MAX_MEM_MIB}")));
		}
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		let record = self
			.inner
			.registry
			.get(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		if record.status != "running" && record.status != "stopped" {
			return Err(EngineError::busy(format!(
				"sandbox '{id}' cannot be resized while {}",
				record.status
			)));
		}
		let current_cpus = record
			.detail
			.get("cpus")
			.and_then(Value::as_u64)
			.and_then(|value| u32::try_from(value).ok())
			.unwrap_or(1);
		let current_memory = record
			.detail
			.get("memory")
			.and_then(Value::as_u64)
			.and_then(|value| u32::try_from(value).ok())
			.unwrap_or(512);
		let current_disk = record
			.detail
			.get("disk_mb")
			.and_then(Value::as_u64)
			.ok_or_else(|| EngineError::invalid("sandbox disk size metadata is missing"))?;
		if disk_mb.is_some_and(|disk| disk < current_disk) {
			return Err(EngineError::invalid("disk_mb cannot shrink the root disk"));
		}
		let target_disk = disk_mb.unwrap_or(current_disk);
		let target_disk_u32 = u32::try_from(target_disk)
			.map_err(|_| EngineError::invalid("disk_mb exceeds the supported VM size"))?;
		let new_bytes = target_disk
			.checked_mul(1024 * 1024)
			.ok_or_else(|| EngineError::invalid("disk_mb exceeds the supported host file size"))?;
		let disk_growth = target_disk > current_disk;
		let vm = self.sandbox(&record.name);
		if !vm.dir().is_dir() || !vm.dir().join("rootfs.img").is_file() {
			return Err(EngineError::not_found(format!("sandbox '{id}' has no retained rootfs.img")));
		}
		if disk_growth {
			self.ensure_disk_resize_available()?;
		}
		let old_recipe = self.recipe_for_cold_start(&record)?;
		let mut new_recipe = old_recipe.clone();
		new_recipe.params.cpus = cpus.unwrap_or(current_cpus);
		new_recipe.params.memory = memory_mib.unwrap_or(current_memory);
		new_recipe.params.disk_mb = target_disk_u32;
		let was_running = record.status == "running";
		if was_running {
			self.stop_locked(id, None)?;
		}

		let rootfs = vm.dir().join("rootfs.img");
		let stage = vm.dir().join("rootfs.img.resize-tmp");
		let backup = vm.dir().join("rootfs.img.pre-resize");
		if disk_growth {
			if let Err(error) = self.build_resized_disk(&rootfs, &stage, new_bytes) {
				let _ = fs::remove_file(&stage);
				if was_running {
					let _ = self.cold_start(id);
				}
				return Err(error);
			}
			let _ = fs::remove_file(&backup);
			if let Err(error) = fs::rename(&rootfs, &backup).and_then(|()| fs::rename(&stage, &rootfs))
			{
				if backup.is_file() && !rootfs.is_file() {
					let _ = fs::rename(&backup, &rootfs);
				}
				let _ = fs::remove_file(&stage);
				if was_running {
					let _ = self.cold_start(id);
				}
				return Err(EngineError::invalid(format!("installing resized disk failed: {error}")));
			}
		}

		let update_result = self
			.inner
			.registry
			.update_detail_persisted(self.home(), id, |detail| {
				detail.insert("cpus".to_owned(), json!(new_recipe.params.cpus));
				detail.insert("memory".to_owned(), json!(new_recipe.params.memory));
				detail.insert("disk_mb".to_owned(), json!(new_recipe.params.disk_mb));
				if detail.get("relaunch_recipe").is_some() {
					let mut persisted_params = new_recipe.params.clone();
					persisted_params.secrets = None;
					detail.insert(
						"relaunch_recipe".to_owned(),
						json!({
							"params": persisted_params,
							"template_dir": new_recipe.template_dir,
							"image_ref": new_recipe.image_ref,
						}),
					);
				}
			});
		if let Err(error) = update_result {
			if disk_growth {
				let _ = fs::remove_file(&rootfs);
				let _ = fs::rename(&backup, &rootfs);
			}
			if was_running {
				let _ = self.cold_start(id);
			}
			return Err(error);
		}
		self
			.inner
			.relaunch_recipes
			.lock()
			.insert(id.to_owned(), new_recipe);
		if was_running && let Err(error) = self.cold_start(id) {
			if disk_growth {
				let _ = fs::remove_file(&rootfs);
				let _ = fs::rename(&backup, &rootfs);
			}
			self
				.inner
				.relaunch_recipes
				.lock()
				.insert(id.to_owned(), old_recipe.clone());
			let _ = self
				.inner
				.registry
				.update_detail_persisted(self.home(), id, |detail| {
					detail.insert("cpus".to_owned(), json!(current_cpus));
					detail.insert("memory".to_owned(), json!(current_memory));
					detail.insert("disk_mb".to_owned(), json!(current_disk));
					if detail.get("relaunch_recipe").is_some() {
						let mut persisted_params = old_recipe.params.clone();
						persisted_params.secrets = None;
						detail.insert(
							"relaunch_recipe".to_owned(),
							json!({
								"params": persisted_params,
								"template_dir": old_recipe.template_dir,
								"image_ref": old_recipe.image_ref,
							}),
						);
					}
				});
			let _ = self.cold_start(id);
			return Err(error);
		}
		if disk_growth {
			let _ = fs::remove_file(&backup);
		}
		Ok(self.get_record(id, false)?.view())
	}

	fn suspend(&self, id: &str) -> Result<Value> {
		let policy_record = self.get_record(id, true)?;
		if policy_record.persistence == PersistencePolicy::Ephemeral {
			return self.stop_with_returncode(id, None);
		}
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		let record = self.get_record(id, true)?;
		let transition = self.begin_state_transition(id, LifecyclePhase::Suspended)?;
		if transition.disposition != TransitionDisposition::Acquired {
			return Ok(self.get_record(id, false)?.view());
		}
		let generation = transition.generation;
		let suspend = match self.lifecycle_handoff_owner(id) {
			Ok(suspend) => suspend,
			Err(error) => {
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			},
		};
		let suspend_handoff = suspend.as_ref().map(|suspend| Arc::clone(&suspend.handoff));
		let capture_owner = self.inner.portable_history.as_ref().and_then(|_| {
			suspend
				.as_ref()
				.map(|suspend| (suspend.owner.clone(), suspend.epoch))
		});
		let recovery_point = match self.capture_recovery_with_portability_unlocked(
			id,
			"checkpoint",
			false,
			true,
			Some(generation.0),
		) {
			Ok(point) => point,
			Err(error) => {
				let may_resume = if let Some((_owner, _epoch)) = &capture_owner {
					let ownership = self.inner.portable_ownership.lock();
					matches!(
						ownership
							.as_ref()
							.ok_or_else(|| EngineError::engine(
								"portable suspend ownership bridge disappeared"
							))
							.and_then(|portable| portable.suspend_marker(id)),
						Ok(None)
					)
				} else {
					true
				};
				if may_resume
					&& let Err(abort_error) = self.abort_suspend_then_resume(
						id,
						suspend_handoff.as_ref(),
						capture_owner.as_ref(),
					) {
					self.fail_state_transition(id, generation, &abort_error);
					return Err(abort_error);
				}
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			},
		};
		let suspend_owner = if let Some(suspend) = &suspend {
			if let Err(error) = suspend.handoff.prepare_suspend(
				id,
				&suspend.owner,
				suspend.epoch,
				&recovery_point.name,
				generation.0,
			) {
				let owner_identity = (suspend.owner.clone(), suspend.epoch);
				if let Err(abort_error) =
					self.abort_suspend_then_resume(id, Some(&suspend.handoff), Some(&owner_identity))
				{
					self.fail_state_transition(id, generation, &abort_error);
					return Err(abort_error);
				}
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			}
			Some((suspend.owner.clone(), suspend.epoch))
		} else {
			None
		};
		// The full checkpoint remains paused until this exact name is durable
		// in local/portable history. Record the name before source teardown so
		// a restart never has to guess among concurrent history captures.
		if let Err(error) = self.mesh_update_detail_fields(
			id,
			Map::from_iter([
				("suspend_recovery_point".to_owned(), json!(recovery_point.name)),
				("suspend_recovery_generation".to_owned(), json!(generation.0)),
			]),
		) {
			if let Err(abort_error) =
				self.abort_suspend_then_resume(id, suspend_handoff.as_ref(), suspend_owner.as_ref())
			{
				self.fail_state_transition(id, generation, &abort_error);
				return Err(abort_error);
			}
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		let returncode = match self.teardown(&record) {
			Ok(code) => code,
			Err(error) => {
				// Teardown can have killed the VMM before a later cleanup
				// failure. The helper probes liveness and only aborts/resumes a
				// demonstrably live source.
				if let Err(abort_error) =
					self.abort_suspend_then_resume(id, suspend_handoff.as_ref(), suspend_owner.as_ref())
				{
					self.fail_state_transition(id, generation, &abort_error);
					return Err(abort_error);
				}
				self.fail_state_transition(id, generation, &error);
				return Err(error);
			},
		};
		if let Some(handoff) = &suspend_handoff
			&& let Err(error) = self
				.inner
				.net_runtime
				.block_on(handoff.release_source_volume_leases(id))
		{
			// The durable suspending marker remains the recovery authority; do
			// not falsely commit suspension while the old writable votes remain.
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		if let Err(error) = self.persist_status(id, "suspended", returncode, None) {
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		if let (Some(handoff), Some((owner, epoch))) = (&suspend_handoff, &suspend_owner)
			&& let Err(error) = handoff.commit_suspend(id, owner, *epoch)
		{
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		self.complete_state_transition(id, generation, LifecyclePhase::Suspended)?;
		let record = self
			.inner
			.registry
			.get(id)
			.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
		self.publish_record_event("suspended", &record);
		Ok(record.view())
	}

	fn history(&self, id: &str) -> Result<Vec<RecoveryPoint>> {
		if self.inner.portable_history.is_some() {
			return self.recovery_points(id);
		}
		self.get_record(id, false)?;
		self.prune_recovery(id, None)?;
		self.recovery_points(id)
	}

	fn rollback(&self, id: &str, recovery_point: &str) -> Result<Value> {
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		if self.inner.registry.get(id).is_none() {
			let handoff = self
				.inner
				.restore_handoff
				.lock()
				.as_ref()
				.and_then(Weak::upgrade);
			if self.inner.portable_history.is_some() && handoff.is_none() {
				return Err(EngineError::engine("portable rollback requires an ownership handoff"));
			}
			// Download and validate bytes before atomically fencing the observed
			// owner into a durable `rolling_back` intent for this exact target.
			// The marker remains recoverable if this process dies before its
			// replacement reaches `commit_rollback`.
			let (source, mut params) = self.open_recovery(id, recovery_point)?;
			let lifecycle_generation = params
				.get("state_generation")
				.and_then(Value::as_u64)
				.ok_or_else(|| {
					EngineError::engine("portable recovery manifest has no lifecycle generation")
				})?;
			let checkpoint_generation = params
				.get("checkpoint_generation")
				.and_then(Value::as_u64)
				.unwrap_or(lifecycle_generation);
			let lease = {
				let mut ownership = self.inner.portable_ownership.lock();
				if ownership.is_none() {
					*ownership = PortableOwnership::connect(&self.inner.config)?;
				}
				let ownership = ownership
					.as_ref()
					.ok_or_else(|| EngineError::not_found(format!("unknown sandbox '{id}'")))?;
				let (owner, epoch) = ownership.current(id)?;
				ownership.claim_expected_rollback(
					id,
					&owner,
					epoch,
					recovery_point,
					lifecycle_generation,
					checkpoint_generation,
				)?
			};
			let owns_lease = {
				let ownership = self.inner.portable_ownership.lock();
				match ownership
					.as_ref()
					.ok_or_else(|| EngineError::engine("portable ownership bridge disappeared"))
					.and_then(|ownership| ownership.verify(id, &lease.owner_node, lease.epoch))
				{
					Ok(verified) => verified,
					Err(error) => {
						let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
						return Err(error);
					},
				}
			};
			if !owns_lease {
				let ownership = self.inner.portable_ownership.lock();
				let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
				return Err(EngineError::busy("portable rollback ownership was superseded"));
			}
			if let Some(handoff) = &handoff
				&& let Err(error) = handoff.begin_restore(id, &lease.owner_node, lease.epoch)
			{
				let ownership = self.inner.portable_ownership.lock();
				let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
				return Err(error);
			}
			params.insert("name".to_owned(), json!(id));
			let mut volume_leases = match handoff.as_ref() {
				Some(handoff) => match RestoreVolumeLeases::acquire(
					self.inner.net_runtime.handle().clone(),
					Arc::clone(handoff),
					id,
					&lease.owner_node,
					lease.epoch,
					&mut params,
				) {
					Ok(leases) => Some(leases),
					Err(error) => {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
						let ownership = self.inner.portable_ownership.lock();
						let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
						return Err(error);
					},
				},
				None => None,
			};
			let heartbeat = {
				let ownership = self.inner.portable_ownership.lock();
				match ownership
					.as_ref()
					.ok_or_else(|| EngineError::engine("portable ownership bridge disappeared"))
					.and_then(|ownership| ownership.start_restore_heartbeat(&lease))
				{
					Ok(heartbeat) => heartbeat,
					Err(error) => {
						if let Some(handoff) = &handoff {
							let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
						}
						let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
						return Err(error);
					},
				}
			};
			self
				.inner
				.launch_cancellations
				.lock()
				.insert(id.to_owned(), heartbeat.lost_signal());
			let restore_engine = self.clone();
			let restore_path = source.path.clone();
			let launch = match thread::Builder::new()
				.name(format!("portable-restore-{id}"))
				.spawn(move || restore_engine.restore_from_template(params, &restore_path, false))
			{
				Ok(launch) => launch,
				Err(error) => {
					self.inner.launch_cancellations.lock().remove(id);
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let ownership = self.inner.portable_ownership.lock();
					let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
					return Err(EngineError::engine(format!("starting portable restore: {error}")));
				},
			};
			// Always join the launch before consuming the authority result. A
			// heartbeat may report loss while the synchronous materialization is
			// unwinding, but it must never leave an unobserved worker behind.
			let Ok(launch_result) = launch.join() else {
				self.inner.launch_cancellations.lock().remove(id);
				self.remove_restore_candidate(id)?;
				if let Some(handoff) = &handoff {
					let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
				}
				let ownership = self.inner.portable_ownership.lock();
				let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
				return Err(EngineError::engine("portable restore worker panicked"));
			};
			self.inner.launch_cancellations.lock().remove(id);
			let authorized = match self.inner.net_runtime.block_on(heartbeat.finish()) {
				Ok(authorized) => authorized,
				Err(error) => {
					self.remove_restore_candidate(id)?;
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let ownership = self.inner.portable_ownership.lock();
					let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
					return Err(error);
				},
			};
			let view = match launch_result {
				Ok(view) if authorized => view,
				Ok(_) => {
					self.remove_restore_candidate(id)?;
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let ownership = self.inner.portable_ownership.lock();
					let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
					return Err(EngineError::busy("portable rollback lease was lost during restore"));
				},
				Err(error) => {
					self.remove_restore_candidate(id)?;
					if let Some(handoff) = &handoff {
						let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
					}
					let ownership = self.inner.portable_ownership.lock();
					let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
					return Err(error);
				},
			};
			// `restore_from_template` constructs a fresh local record. Before
			// finalizing PostgreSQL ownership, restore the recovery point's
			// generation as a steady Running state so a new owner never rewinds
			// lifecycle CAS state back to the constructor's generation one.
			let Some(mut restored) = self.inner.registry.get(id) else {
				if let Some(handoff) = &handoff {
					let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
				}
				let ownership = self.inner.portable_ownership.lock();
				let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
				return Err(EngineError::engine("portable restore created no registry record"));
			};
			restored.lifecycle = LifecycleState {
				desired:    LifecyclePhase::Running,
				observed:   LifecyclePhase::Running,
				generation: StateGeneration(lifecycle_generation),
				operation:  None,
				failure:    None,
			};
			if !restored.detail.is_object() {
				restored.detail = Value::Object(Map::new());
			}
			let Some(detail) = restored.detail.as_object_mut() else {
				if let Some(handoff) = &handoff {
					let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
				}
				let ownership = self.inner.portable_ownership.lock();
				let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
				return Err(EngineError::engine("restored record detail cannot be materialized"));
			};
			detail.insert("state_generation".to_owned(), json!(lifecycle_generation));
			if let Err(error) = self.inner.registry.insert_persisted(self.home(), restored) {
				if let Err(teardown_error) = self.remove_restore_candidate(id) {
					self
						.inner
						.runtimes
						.lock()
						.entry(id.to_owned())
						.or_default()
						.restore_volume_leases = volume_leases.take();
					return Err(teardown_error);
				}
				if let Some(handoff) = &handoff {
					let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
				}
				let ownership = self.inner.portable_ownership.lock();
				let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
				return Err(error);
			}
			if let Some(leases) = volume_leases.as_ref()
				&& let Err(error) = leases.persist(id)
			{
				self
					.inner
					.runtimes
					.lock()
					.entry(id.to_owned())
					.or_default()
					.restore_volume_leases = volume_leases.take();
				self.remove_restore_candidate(id)?;
				if let Some(handoff) = &handoff {
					let _ = handoff.abort_restore(id, &lease.owner_node, lease.epoch);
				}
				let ownership = self.inner.portable_ownership.lock();
				let _ = ownership.as_ref().map(|ownership| ownership.abort(&lease));
				return Err(error);
			}
			if let Some(handoff) = &handoff
				&& let Err(error) = handoff.commit_rollback(id, &lease.owner_node, lease.epoch)
				&& handoff
					.commit_rollback(id, &lease.owner_node, lease.epoch)
					.is_err()
			{
				if let Some(leases) = volume_leases.take() {
					self
						.inner
						.runtimes
						.lock()
						.entry(id.to_owned())
						.or_default()
						.restore_volume_leases = Some(leases);
				}
				// Preserve candidate, fenced votes, and marker for
				// recovery; the first commit may have succeeded.
				return Err(error);
			}
			if let Some(volume_leases) = &mut volume_leases {
				volume_leases.disarm();
			}
			if let Some(guard) = source.guard {
				self
					.inner
					.runtimes
					.lock()
					.entry(id.to_owned())
					.or_default()
					.snapshot_source = Some(guard);
			}
			self.mesh_update_detail_fields(
				id,
				Map::from_iter([
					("portable_owner".to_owned(), json!(lease.owner_node)),
					("portable_owner_epoch".to_owned(), json!(lease.epoch)),
					("recovery_point".to_owned(), json!(recovery_point)),
				]),
			)?;
			return Ok(view);
		}
		// Validate the selected replacement before claiming an operation, but
		// never create a hidden safety checkpoint for a joined duplicate.
		self.open_recovery(id, recovery_point)?;
		let record = self.get_record(id, false)?;
		let transition = self.inner.registry.begin_operation(
			self.home(),
			id,
			record.lifecycle.generation,
			LifecyclePhase::Running,
			Some(LifecycleOperation::Rollback { recovery_point: recovery_point.to_owned() }),
		)?;
		if transition.disposition != TransitionDisposition::Acquired {
			return Ok(self.get_record(id, false)?.view());
		}
		let generation = transition.generation;
		let rollback_handoff = self.inner.portable_history.as_ref().and_then(|_| {
			self
				.inner
				.restore_handoff
				.lock()
				.as_ref()
				.and_then(Weak::upgrade)
		});
		let rollback_owner = if self.inner.portable_history.is_some() {
			let mut ownership = self.inner.portable_ownership.lock();
			if ownership.is_none() {
				*ownership = PortableOwnership::connect(&self.inner.config)?;
			}
			ownership
				.as_ref()
				.ok_or_else(|| EngineError::engine("portable rollback ownership bridge disappeared"))?
				.current(id)?
		} else {
			("local".to_owned(), 0)
		};
		let rollback_source_token = random_hex(16);
		if let Err(error) = self.mesh_update_detail_fields(
			id,
			Map::from_iter([
				("lifecycle_operation".to_owned(), json!("rollback")),
				("rollback_recovery_point".to_owned(), json!(recovery_point)),
				("rollback_generation".to_owned(), json!(generation.0)),
				("rollback_owner".to_owned(), json!(rollback_owner.0)),
				("rollback_owner_epoch".to_owned(), json!(rollback_owner.1)),
				("rollback_source_token".to_owned(), json!(rollback_source_token)),
			]),
		) {
			let _ = self.inner.registry.cancel_transition(
				self.home(),
				id,
				generation,
				LifecyclePhase::Running,
				error.to_string(),
			);
			self.clear_rollback_detail(id);
			return Err(error);
		}
		if self.inner.portable_history.is_some() && rollback_handoff.is_none() {
			let error = EngineError::engine("portable rollback requires an ownership handoff");
			let _ = self.inner.registry.cancel_transition(
				self.home(),
				id,
				generation,
				LifecyclePhase::Running,
				error.to_string(),
			);
			self.clear_rollback_detail(id);
			return Err(error);
		}
		if let Some(history) = &self.inner.portable_history
			&& let Err(error) = history.pin_rollback_target(
				id,
				recovery_point,
				generation.0,
				&rollback_owner.0,
				rollback_owner.1,
			) {
			let _ = self.inner.registry.cancel_transition(
				self.home(),
				id,
				generation,
				LifecyclePhase::Running,
				error.to_string(),
			);
			self.clear_rollback_detail(id);
			return Err(error);
		}
		// Own the only rollback failure-resume path before capture begins.
		// It is fenced to the source PID, so a later replacement cannot be
		// resumed accidentally.
		let mut rollback_resume_on_error = self.rollback_resume_guard(id);
		let safety_recovery_point = match self.capture_recovery_with_portability_unlocked(
			id,
			"rollback-safety",
			false,
			self.inner.portable_history.is_some(),
			None,
		) {
			Ok(point) => point.name,
			Err(error) => {
				// `rollback_resume_on_error` resumes the still-original source.
				if let Some(history) = &self.inner.portable_history {
					let _ = history.release_rollback_target(
						id,
						recovery_point,
						generation.0,
						&rollback_owner.0,
						rollback_owner.1,
					);
				}
				let _ = self.inner.registry.cancel_transition(
					self.home(),
					id,
					generation,
					LifecyclePhase::Running,
					error.to_string(),
				);
				self.clear_rollback_detail(id);
				return Err(error);
			},
		};
		let current = self.get_record(id, false)?;
		let journal = RollbackJournal {
			sandbox_id: id.to_owned(),
			target_recovery_point: recovery_point.to_owned(),
			safety_recovery_point,
			generation: generation.0,
			checkpoint_generation: current
				.detail
				.get("checkpoint_generation")
				.and_then(Value::as_u64)
				.unwrap_or(0),
			portable_owner: rollback_owner.0.clone(),
			portable_owner_epoch: rollback_owner.1,
			source_token: rollback_source_token,
		};
		if let Err(error) = self.write_rollback_journal(&journal) {
			let _ = self.remove_safety_recovery(id, &journal.safety_recovery_point);
			if let Some(history) = &self.inner.portable_history {
				let _ = history.release_rollback_target(
					id,
					recovery_point,
					generation.0,
					&rollback_owner.0,
					rollback_owner.1,
				);
			}
			let _ = self.inner.registry.cancel_transition(
				self.home(),
				id,
				generation,
				LifecyclePhase::Running,
				error.to_string(),
			);
			self.clear_rollback_detail(id);
			return Err(error);
		}
		if let Some(handoff) = &rollback_handoff
			&& let Err(error) = handoff.prepare_rollback(
				id,
				&journal.portable_owner,
				journal.portable_owner_epoch,
				&journal.target_recovery_point,
				&journal.safety_recovery_point,
				journal.generation,
				journal.checkpoint_generation,
			) {
			let _ = self.clear_rollback_journal(id);
			let _ = self.release_rollback_pin(&journal);
			let _ = self.inner.registry.cancel_transition(
				self.home(),
				id,
				generation,
				LifecyclePhase::Running,
				error.to_string(),
			);
			self.clear_rollback_detail(id);
			return Err(error);
		}
		let view = match self.restore_recovery_identity(
			id,
			recovery_point,
			rollback_handoff.as_ref().map(|handoff| {
				(Arc::clone(handoff), journal.portable_owner.clone(), journal.portable_owner_epoch)
			}),
		) {
			Ok(view) => view,
			Err(target_error) => {
				// The target was validated before teardown, but launch can still
				// fail. Restore the independently durable safety point before
				// admitting failure so the caller never loses a runnable
				// identity to a failed rollback.
				if self
					.restore_recovery_identity(
						id,
						&journal.safety_recovery_point,
						rollback_handoff.as_ref().map(|handoff| {
							(
								Arc::clone(handoff),
								journal.portable_owner.clone(),
								journal.portable_owner_epoch,
							)
						}),
					)
					.is_ok()
				{
					if let Some(handoff) = &rollback_handoff
						&& let Err(error) = handoff.commit_rollback(
							id,
							&journal.portable_owner,
							journal.portable_owner_epoch,
						) && handoff
						.commit_rollback(id, &journal.portable_owner, journal.portable_owner_epoch)
						.is_err()
					{
						self.fail_state_transition(id, generation, &error);
						return Err(error);
					}
					self.disarm_restore_volume_leases(id);
					self.inner.registry.cancel_transition(
						self.home(),
						id,
						generation,
						LifecyclePhase::Running,
						"rollback target failed; safety recovery restored",
					)?;
					self.finalize_rollback_journal(&journal)?;
					self.clear_rollback_detail(id);
					return Err(target_error);
				}
				self.fail_state_transition(id, generation, &target_error);
				return Err(target_error);
			},
		};
		// The replacement is now running. It must never be resumed through the
		// old source's guard, even if the following durable commit is ambiguous.
		rollback_resume_on_error.disarm();
		if let Some(handoff) = &rollback_handoff
			&& let Err(error) =
				handoff.commit_rollback(id, &journal.portable_owner, journal.portable_owner_epoch)
			&& handoff
				.commit_rollback(id, &journal.portable_owner, journal.portable_owner_epoch)
				.is_err()
		{
			self.fail_state_transition(id, generation, &error);
			return Err(error);
		}
		self.disarm_restore_volume_leases(id);
		self.complete_state_transition(id, generation, LifecyclePhase::Running)?;
		self.remove_safety_recovery(id, &journal.safety_recovery_point)?;
		self.release_rollback_pin(&journal)?;
		self.clear_rollback_journal(id)?;
		self.clear_rollback_detail(id);
		if let Some(record) = self.inner.registry.get(id) {
			self.publish_record_event("rollback", &record);
		}
		Ok(view)
	}

	fn extend(&self, id: &str, secs: u64) -> Result<Value> {
		self.get_record(id, true)?;
		let deadline = control_for_vm(&self.sandbox(id))?.extend(secs)?;
		self.inner.registry.update(id, |record| {
			record.timeout = Some(secs as f64);
			record.touch();
		});
		let _ = self
			.inner
			.registry
			.update_detail_persisted(self.home(), id, |detail| {
				detail.insert("timeout_secs".to_owned(), json!(secs));
			});
		if let Some(state) = self.inner.runtimes.lock().get_mut(id) {
			if let Some(stop) = state.timeout_stop.take() {
				let _ = stop.send(());
			}
			state.timeout_stop = Some(start_timeout_watchdog(
				id.to_owned(),
				secs,
				Arc::clone(&self.inner.sandbox_runtime),
			));
		}
		Ok(json!({ "deadline_unix": deadline }))
	}

	fn set_idle_timeout(&self, id: &str, secs: f64) -> Result<Value> {
		if !secs.is_finite() || secs < 0.0 {
			return Err(EngineError::invalid("idle_timeout_secs must be non-negative"));
		}
		let _maintenance_permit = self.maintenance_permit(id);
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		self.get_record(id, false)?;
		let record =
			self
				.inner
				.registry
				.set_idle_timeout_persisted(self.home(), id, secs, unix_time())?;
		if let Some(recipe) = self.inner.relaunch_recipes.lock().get_mut(id) {
			recipe.params.idle_timeout_secs = Some(secs);
		}
		self.wake_maintenance();
		Ok(record.view())
	}

	fn metrics(&self, id: &str) -> Result<Value> {
		self.get_record(id, true)?;
		control_for_vm(&self.sandbox(id))?.metrics()
	}

	fn logs(&self, id: &str) -> Result<Vec<u8>> {
		self.get_record(id, false)?;
		Ok(fs::read(self.sandbox(id).log_path()).unwrap_or_default())
	}

	fn logs_follow(&self, id: &str) -> Result<Receiver<Vec<u8>>> {
		self.get_record(id, false)?;
		let name = id.to_owned();
		let path = self.sandbox(id).log_path().to_path_buf();
		let (tx, rx) = flume::unbounded();
		let registry = Arc::clone(&self.inner);
		thread::Builder::new()
			.name(format!("vmon-log-follow-{id}"))
			.spawn(move || {
				let mut offset = 0;
				loop {
					if let Ok(mut file) = fs::File::open(&path)
						&& file.seek(SeekFrom::Start(offset)).is_ok()
					{
						let mut buf = Vec::new();
						if io::Read::read_to_end(&mut file, &mut buf).is_ok() && !buf.is_empty() {
							offset += buf.len() as u64;
							if tx.send(buf).is_err() {
								return;
							}
						}
					}
					let terminal = registry
						.registry
						.get(&name)
						.is_some_and(|record| record.status != "running")
						|| {
							let vm = registry.sandbox_runtime.sandbox(&name);
							!registry.sandbox_runtime.is_running(&vm).unwrap_or(false)
						};
					if terminal {
						return;
					}
					thread::sleep(LOG_FOLLOW_POLL);
				}
			})?;
		Ok(rx)
	}

	fn exec_capture(&self, id: &str, req: ExecRequest) -> Result<ExecCapture> {
		if req.cmd.is_empty() {
			return Err(EngineError::invalid("exec cmd must not be empty"));
		}
		self.get_record(id, true)?;
		self.inc_counter("exec");
		let timeout = clamp_exec_timeout(req.timeout);
		let agent = self.agent_for(id)?;
		let env = self
			.inner
			.runtimes
			.lock()
			.get(id)
			.map_or_else(BTreeMap::new, |state| merged_env(state, req.env.as_ref()));
		let fallback_workdir = self
			.inner
			.runtimes
			.lock()
			.get(id)
			.and_then(|state| state.workdir.clone());
		let workdir = req.workdir.as_deref().or(fallback_workdir.as_deref());
		let session =
			agent.exec(&req.cmd, workdir.map(Path::new), Some(&env), req.tty, Some(timeout))?;
		let stdout = session.stdout.iter().flatten().collect();
		let stderr = session.stderr.iter().flatten().collect();
		let exit = session.wait(Some(timeout))?;
		Ok(ExecCapture { exit, stdout, stderr })
	}

	fn exec_stream(&self, id: &str, req: ExecRequest) -> Result<ExecStream> {
		if req.cmd.is_empty() {
			return Err(EngineError::invalid("exec cmd must not be empty"));
		}
		self.get_record(id, true)?;
		self.inc_counter("exec");
		let agent = self.agent_for(id)?;
		let env = self
			.inner
			.runtimes
			.lock()
			.get(id)
			.map_or_else(BTreeMap::new, |state| merged_env(state, req.env.as_ref()));
		let fallback_workdir = self
			.inner
			.runtimes
			.lock()
			.get(id)
			.and_then(|state| state.workdir.clone());
		let workdir = req.workdir.as_deref().or(fallback_workdir.as_deref());
		let session = agent.exec(
			&req.cmd,
			workdir.map(Path::new),
			Some(&env),
			req.tty,
			req.timeout.map(Duration::from_secs_f64),
		)?;
		let parts = session.split();
		let stdout = bridge_bytes(parts.stdout);
		let stderr = bridge_bytes(parts.stderr);
		let exit = bridge_exit(parts.exit);
		Ok(ExecStream {
			control: Box::new(EngineExecControl { handle: parts.control }),
			stdout,
			stderr,
			exit,
		})
	}

	fn pty_open(&self, id: &str, params: Value) -> Result<PtyStream> {
		self.get_record(id, true)?;
		let parts = self
			.agent_for(id)?
			.pty_open(params, AGENT_REQUEST_TIMEOUT)?;
		self.inner.pty_cache.remember(id, [parts.session.clone()]);
		Ok(PtyStream {
			session: parts.session,
			control: Box::new(EnginePtyControl { handle: parts.control }),
			stdout:  bridge_bytes(parts.stdout),
			exit:    bridge_exit(parts.exit),
		})
	}

	fn pty_attach(&self, id: &str, params: Value) -> Result<PtyStream> {
		let record = self.get_record(id, false)?;
		if record.status == "suspended" {
			self.resume(id)?;
		}
		let parts = self
			.agent_for(id)?
			.pty_attach(params, AGENT_REQUEST_TIMEOUT)?;
		self.inner.pty_cache.remember(id, [parts.session.clone()]);
		Ok(PtyStream {
			session: parts.session,
			control: Box::new(EnginePtyControl { handle: parts.control }),
			stdout:  bridge_bytes(parts.stdout),
			exit:    bridge_exit(parts.exit),
		})
	}

	fn pty_list(&self, id: &str) -> Result<Vec<Value>> {
		let record = self.get_record(id, false)?;
		if record.status == "suspended" {
			return Ok(self.inner.pty_cache.suspended(id));
		}
		let sessions = self.agent_for(id)?.pty_list(AGENT_REQUEST_TIMEOUT)?;
		self.inner.pty_cache.replace(id, sessions.clone());
		Ok(sessions)
	}

	fn pty_close(&self, id: &str, session_id: &str) -> Result<Value> {
		self.get_record(id, true)?;
		let response = self
			.agent_for(id)?
			.pty_close(session_id, AGENT_REQUEST_TIMEOUT)?;
		self.inner.pty_cache.forget(id, session_id);
		Ok(response)
	}

	fn pty_exec(
		&self,
		id: &str,
		session_id: &str,
		command: &str,
		timeout: f64,
	) -> Result<ExecCapture> {
		if !timeout.is_finite() || timeout <= 0.0 {
			return Err(EngineError::invalid("PTY exec timeout must be positive and finite"));
		}
		self.get_record(id, true)?;
		let response =
			self
				.agent_for(id)?
				.pty_exec(session_id, command, Duration::from_secs_f64(timeout))?;
		let stdout = serde_json::from_value(response.get("stdout").cloned().unwrap_or_default())?;
		let stderr = serde_json::from_value(response.get("stderr").cloned().unwrap_or_default())?;
		let exit = response.get("code").and_then(Value::as_i64).unwrap_or(-1);
		Ok(ExecCapture { exit, stdout, stderr })
	}

	fn shell_start(&self, params: Value) -> Result<ShellSession> {
		let ref_name = params.get("ref").and_then(Value::as_str);
		let required_owner = params.get("_vmon_owner_tenant").and_then(Value::as_str);
		let image = params
			.get("image")
			.and_then(Value::as_str)
			.map(ToOwned::to_owned);
		let cmd = params
			.get("cmd")
			.and_then(Value::as_array)
			.map(|items| {
				items
					.iter()
					.filter_map(Value::as_str)
					.map(ToOwned::to_owned)
					.collect::<Vec<_>>()
			})
			.filter(|cmd| !cmd.is_empty())
			.unwrap_or_else(|| {
				DEFAULT_SHELL_ARGV
					.iter()
					.map(|part| (*part).to_owned())
					.collect()
			});
		if let Some(ref_name) = ref_name {
			match self.get_record(ref_name, true) {
				Ok(record) => {
					let owner = record
						.detail
						.get("owner_tenant")
						.and_then(Value::as_str)
						.unwrap_or("default");
					if required_owner.is_some_and(|required| required != owner) {
						return Err(EngineError::not_found(format!(
							"sandbox {ref_name:?} does not exist"
						)));
					}
					let stream = self.exec_stream(&record.id, ExecRequest {
						cmd,
						tty: true,
						..ExecRequest::default()
					})?;
					return Ok(ShellSession { name: record.name, stream, ephemeral: false });
				},
				Err(_) if required_owner.is_some() => {
					return Err(EngineError::not_found(format!("sandbox {ref_name:?} does not exist")));
				},
				Err(_) => {},
			}
		}
		let mut create = SandboxCreate {
			image: image
				.or_else(|| ref_name.map(ToOwned::to_owned))
				.or_else(|| Some(DEFAULT_SHELL_IMAGE.to_owned())),
			cpus: params.get("cpus").and_then(Value::as_u64).unwrap_or(1) as u32,
			memory: params.get("mem").and_then(Value::as_u64).unwrap_or(512) as u32,
			disk_mb: params
				.get("disk_mb")
				.and_then(Value::as_u64)
				.unwrap_or(1024) as u32,
			timeout: Some(
				params
					.get("timeout")
					.and_then(Value::as_f64)
					.unwrap_or(300.0),
			),
			block_network: true,
			..SandboxCreate::default()
		};
		if let Some(ref_name) = ref_name
			&& self.home().templates_dir().join(ref_name).exists()
		{
			create.template = Some(ref_name.to_owned());
			create.image = None;
		}
		let view = self.create(create)?;
		let name = view
			.get("name")
			.and_then(Value::as_str)
			.ok_or_else(|| EngineError::engine("shell setup did not return a VM name"))?
			.to_owned();
		let stream =
			self.exec_stream(&name, ExecRequest { cmd, tty: true, ..ExecRequest::default() })?;
		Ok(ShellSession { name, stream, ephemeral: true })
	}

	fn shell_cleanup(&self, name: &str) {
		let _ = self.remove(name);
	}

	fn file_read(&self, id: &str, path: &str) -> Result<Vec<u8>> {
		self.inc_counter("file_read");
		self.get_record(id, true)?;
		self
			.agent_for(id)?
			.fs_read(Path::new(path), AGENT_REQUEST_TIMEOUT)
	}

	fn file_write(&self, id: &str, path: &str, data: &[u8]) -> Result<()> {
		self.inc_counter("file_write");
		self.get_record(id, true)?;
		self
			.agent_for(id)?
			.fs_write(Path::new(path), data, AGENT_REQUEST_TIMEOUT)?;
		Ok(())
	}

	fn file_delete(&self, id: &str, path: &str, recursive: bool) -> Result<()> {
		self.inc_counter("file_delete");
		self.get_record(id, true)?;
		self
			.agent_for(id)?
			.fs_remove(Path::new(path), recursive, AGENT_REQUEST_TIMEOUT)?;
		Ok(())
	}

	fn file_list(&self, id: &str, path: &str) -> Result<Value> {
		self.get_record(id, true)?;
		Ok(json!(
			self
				.agent_for(id)?
				.fs_list(Path::new(path), AGENT_REQUEST_TIMEOUT)?
		))
	}

	fn file_stat(&self, id: &str, path: &str) -> Result<Value> {
		self.get_record(id, true)?;
		self
			.agent_for(id)?
			.fs_stat(Path::new(path), AGENT_REQUEST_TIMEOUT)
	}

	fn network_get(&self, id: &str) -> Result<Value> {
		let record = self.get_record(id, false)?;
		Ok(json!({
			"block_network": record.detail.get("block_network").cloned().unwrap_or(Value::Null),
			"egress_allow": record.detail.get("egress_allow").cloned().unwrap_or(Value::Null),
			"egress_allow_domains": record.detail.get("egress_allow_domains").cloned().unwrap_or(Value::Null),
			"inbound_cidr_allowlist": record.detail.get("inbound_cidr_allowlist").cloned().unwrap_or(Value::Null),
		}))
	}

	fn network_set(&self, id: &str, policy: NetworkBody) -> Result<Value> {
		validate_cidrs("cidr_allow", policy.cidr_allow.as_deref())?;
		validate_domains("domain_allow", policy.domain_allow.as_deref())?;
		let mut runtimes = self.inner.runtimes.lock();
		if let Some(state) = runtimes.get_mut(id)
			&& let Some(network) = &state.network
		{
			let allow_list = if policy.block_network == Some(true) {
				Some(Vec::new())
			} else {
				policy
					.cidr_allow
					.clone()
					.or_else(|| state.network_policy.egress_allow.clone())
			};
			let domain_list = if policy.block_network == Some(true) {
				Some(Vec::new())
			} else {
				policy
					.domain_allow
					.clone()
					.or_else(|| state.network_policy.egress_allow_domains.clone())
			};
			net::setup_tap(
				&network.guest_config.tap,
				&network.guest_config.guest_ip,
				&network.guest_config.host_ip,
				network.guest_config.prefix,
				allow_list.as_deref(),
				domain_list.as_deref(),
				state.network_policy.egress_allow.as_deref(),
			)?;
			state.network_policy.block_network = policy.block_network.or(Some(false));
			state.network_policy.egress_allow = allow_list;
			state.network_policy.egress_allow_domains = domain_list;
			self.mesh_update_detail_fields(
				id,
				Map::from_iter([
					(
						"block_network".to_owned(),
						json!(state.network_policy.block_network.unwrap_or(false)),
					),
					("egress_allow".to_owned(), json!(state.network_policy.egress_allow)),
					(
						"egress_allow_domains".to_owned(),
						json!(state.network_policy.egress_allow_domains),
					),
				]),
			)?;
			if state.credential_gateway.is_some() {
				network.allow_credential_gateway()?;
			}
		}
		drop(runtimes);
		if let Some(value) = policy.block_network {
			self
				.inner
				.registry
				.set_detail_field(id, "block_network", json!(value));
			if value {
				self
					.inner
					.registry
					.set_detail_field(id, "egress_allow", json!([]));
				self
					.inner
					.registry
					.set_detail_field(id, "egress_allow_domains", json!([]));
			}
		}
		if let Some(value) = policy.cidr_allow {
			self
				.inner
				.registry
				.set_detail_field(id, "egress_allow", json!(value));
		}
		if let Some(value) = policy.domain_allow {
			self
				.inner
				.registry
				.set_detail_field(id, "egress_allow_domains", json!(value));
		}
		self.network_get(id)
	}

	fn tunnels(&self, id: &str) -> Result<Value> {
		self.get_record(id, false)?;
		let mut runtimes = self.inner.runtimes.lock();
		let state = runtimes.entry(id.to_owned()).or_default();
		if state.connect_token.is_none() {
			state.connect_token = Some(random_hex(32));
		}
		let tunnels = state
			.network
			.as_ref()
			.map(SandboxNetwork::tunnels)
			.unwrap_or_default();
		Ok(json!({ "tunnels": tunnels_json(&tunnels), "connect_token": state.connect_token }))
	}

	fn tunnel_target(&self, id: &str, port: u16) -> Result<(String, u16)> {
		self.get_record(id, false)?;
		self
			.inner
			.runtimes
			.lock()
			.get(id)
			.and_then(|state| state.network.as_ref())
			.and_then(|network| network.tunnels().get(&port).cloned())
			.ok_or_else(|| EngineError::engine("no tunnel for sandbox port"))
	}

	fn vpc_create(&self, tenant: &str, name: Option<&str>, cidr: Option<&str>) -> Result<Vpc> {
		net::require_vpc_host()?;
		self.inner.vpcs.create(tenant, name, cidr)
	}

	fn vpc_list(&self, tenant: &str) -> Result<Vec<Vpc>> {
		net::require_vpc_host()?;
		Ok(self.inner.vpcs.list(tenant))
	}

	fn vpc_delete(&self, tenant: &str, id: &str) -> Result<()> {
		net::require_vpc_host()?;
		self.inner.vpcs.ensure_deletable(tenant, id)?;
		net::delete_vpc_bridge(id)?;
		self.inner.vpcs.delete(tenant, id)?;
		Ok(())
	}

	fn snapshot(&self, id: &str, name: Option<String>, stop: bool) -> Result<Value> {
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		let record = self.get_record(id, true)?;
		if !record.lifecycle.is_converged() {
			return Err(EngineError::busy("sandbox lifecycle is not steady for snapshot"));
		}
		let snapshot = name.unwrap_or_else(|| format!("{}-{}", id, unix_millis()));
		let archive = self.snapshot_archive(&snapshot)?;
		if archive.exists() {
			return Err(EngineError::busy(format!("snapshot already exists: {snapshot}")));
		}
		let key_id = record
			.detail
			.get("encryption_key_id")
			.and_then(Value::as_str)
			.unwrap_or("default");
		let vm = self.sandbox(&record.name);
		let disk = vm.rootfs_img().ok().filter(|path| path.is_file());
		let snapshot_root = self.snapshot_root();
		let plaintext_dir = snapshot_root.join(&snapshot);
		let cleanup = TransientDir(plaintext_dir);
		self.snapshot_machine_while_paused(
			&vm,
			&snapshot,
			!stop,
			disk.as_deref(),
			&snapshot_root,
			false,
			false,
			|dir| {
				if let Some(s3_mounts) = record
					.detail
					.get("s3_mounts")
					.filter(|mounts| mounts.as_object().is_some_and(|mounts| !mounts.is_empty()))
				{
					fs::write(dir.join(S3_MOUNTS_FILE), serde_json::to_vec(s3_mounts)?)?;
				}
				EncryptedArchive::seal(dir, &archive, &self.inner.keyring, key_id)
			},
		)?;
		drop(cleanup);
		if stop {
			let rc = self.teardown(&record)?;
			self.persist_status(id, "stopped", rc, None)?;
		}
		self.inc_counter("snapshot");
		self.publish_event(
			"snapshot",
			Map::from_iter([
				("id".to_owned(), json!(record.id)),
				("name".to_owned(), json!(record.name)),
				("snapshot".to_owned(), json!(snapshot)),
			]),
		);
		Ok(json!({ "snapshot": snapshot, "encrypted": true, "key_id": key_id }))
	}

	fn snapshot_fs(&self, id: &str, name: Option<String>) -> Result<Value> {
		let capture_lock = self.capture_lock(id);
		let _capture_guard = capture_lock.acquire();
		let record = self.get_record(id, true)?;
		if !record.lifecycle.is_converged() {
			return Err(EngineError::busy("sandbox lifecycle is not steady for filesystem snapshot"));
		}
		let image = name.unwrap_or_else(|| format!("{id}-img-{}", unix_time() as u64));
		let vm = self.sandbox(id);
		let disk = vm.rootfs_img().ok().filter(|path| path.is_file());
		let src_dir =
			self.snapshot_machine(&vm, &image, true, disk.as_deref(), &self.snapshot_root(), false)?;
		if let Some(s3_mounts) = record
			.detail
			.get("s3_mounts")
			.filter(|mounts| mounts.as_object().is_some_and(|mounts| !mounts.is_empty()))
		{
			fs::write(src_dir.join(S3_MOUNTS_FILE), serde_json::to_vec(s3_mounts)?)?;
		}
		let dst_dir = self.home().templates_dir().join(&image);
		if dst_dir.exists() {
			fs::remove_dir_all(&dst_dir)?;
		}
		fs::rename(&src_dir, &dst_dir)?;
		fs::write(
			dst_dir.join("image.json"),
			serde_json::to_string_pretty(
				&json!({ "created_unix": unix_time() as u64, "ttl": 30 * 24 * 3600, "image": image }),
			)?,
		)?;
		Ok(json!({ "image": image }))
	}

	fn snapshots(&self) -> Result<Vec<String>> {
		let mut names = match fs::read_dir(self.snapshot_root()) {
			Ok(entries) => entries
				.filter_map(std::result::Result::ok)
				.filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_file()))
				.filter_map(|entry| {
					entry
						.file_name()
						.to_str()
						.and_then(|name| name.strip_suffix(".venc"))
						.map(str::to_owned)
				})
				.collect::<Vec<_>>(),
			Err(error) if error.kind() == io::ErrorKind::NotFound => Vec::new(),
			Err(error) => return Err(error.into()),
		};
		names.sort();
		Ok(names)
	}

	fn snapshot_delete(&self, snapshot: &str) -> Result<()> {
		let archive = self.snapshot_archive(snapshot)?;
		let metadata = match fs::symlink_metadata(&archive) {
			Ok(metadata) => metadata,
			Err(error) if error.kind() == io::ErrorKind::NotFound => {
				return Err(EngineError::not_found(format!("snapshot not found: {snapshot}")));
			},
			Err(error) => return Err(error.into()),
		};
		if metadata.file_type().is_symlink() || !metadata.is_file() {
			return Err(EngineError::invalid(format!(
				"snapshot {snapshot:?} is not a regular encrypted archive"
			)));
		}
		self.inner.snapshot_sources.lock().remove(snapshot);
		fs::remove_file(archive)?;
		Ok(())
	}

	fn restore(&self, snapshot: &str, body: RestoreBody) -> Result<Value> {
		let RestoreBody { name, agent, extra } = body;
		let name = name.unwrap_or_else(|| format!("restore-{}", random_hex(12)));
		let options = resolve_snapshot_options(extra, agent)?;
		let source = self.open_snapshot(snapshot)?;
		let s3_mounts = self.snapshot_s3_mounts(&source.path, options.s3_mounts.clone())?;
		let (_, record) = self.launch_snapshot_vm(
			snapshot,
			&source.path,
			name,
			SnapshotLaunchMode::Restore,
			&options,
			&s3_mounts,
			source.guard,
		)?;
		self.publish_record_event("restore", &record);
		Ok(record.view())
	}

	fn fork(&self, snapshot: &str, body: ForkBody) -> Result<Value> {
		let ForkBody { count, extra } = body;
		if !(1..=MAX_FORK_CLONES).contains(&count) {
			return Err(EngineError::invalid(format!(
				"count must be between 1 and {MAX_FORK_CLONES}"
			)));
		}
		let options = resolve_snapshot_options(extra, None)?;
		let source = self.open_snapshot(snapshot)?;
		let s3_mounts = self.snapshot_s3_mounts(&source.path, options.s3_mounts.clone())?;
		let mut launched = Vec::with_capacity(count as usize);
		for _ in 0..count {
			let name = format!("fork-{}", random_hex(12));
			match self.launch_snapshot_vm(
				snapshot,
				&source.path,
				name,
				SnapshotLaunchMode::Fork,
				&options,
				&s3_mounts,
				source.guard.clone(),
			) {
				Ok(launched_vm) => launched.push(launched_vm),
				Err(error) => {
					for (vm, _) in launched.iter().rev() {
						self.rollback_snapshot_vm(vm);
					}
					return Err(error);
				},
			}
		}
		let clones = launched
			.iter()
			.map(|(_, record)| record.view())
			.collect::<Vec<_>>();
		for (_, record) in &launched {
			self.publish_record_event("fork", record);
		}
		self.publish_event(
			"fork",
			Map::from_iter([
				("snapshot".to_owned(), json!(snapshot)),
				("count".to_owned(), json!(count)),
			]),
		);
		Ok(json!({ "clones": clones }))
	}

	fn volume_list(&self) -> Result<Vec<String>> {
		Ok(volumes::list_volumes(self.home().root()))
	}

	fn volume_create(&self, name: &str) -> Result<()> {
		Volume::new_in_home(self.home().root(), name).map(|_| ())
	}

	fn volume_delete(&self, name: &str) -> Result<()> {
		volumes::remove_volume_in_home(self.home().root(), name)
	}

	fn pool_list(&self) -> Result<Value> {
		Ok(json!(self
			.inner
			.pools
			.list()
			.into_iter()
			.map(|(reference, stats)| {
				(reference, json!({ "ready": stats.ready, "hits": stats.hits, "misses": stats.misses, "size": stats.size }))
			})
			.collect::<Map<_, _>>()))
	}

	fn pool_set(&self, reference: &str, body: PoolPutBody) -> Result<Value> {
		let request = template_request_from_pool(reference, &body.extra);
		let cached = image::cached_template(self, &request)?;
		let key = template_key_for_cached(&cached);
		let pool = WarmPool::with_runtime(
			cached.snapshot_dir,
			body.size as usize,
			Arc::clone(&self.inner.sandbox_runtime),
		)?;
		let old = self.inner.pools.set(key.clone(), pool);
		if let Some(old) = old {
			old.shutdown();
		}
		Ok(self
			.pool_list()?
			.get(&key)
			.cloned()
			.unwrap_or_else(|| json!({})))
	}

	fn pool_delete(&self, reference: &str) -> Result<()> {
		if let Some(pool) = self.inner.pools.delete(reference) {
			pool.shutdown();
		}
		Ok(())
	}

	fn credential_list(&self, tenant: &str) -> Result<Vec<CredentialMetadata>> {
		self.inner.credentials.list(tenant)
	}

	fn credential_put(
		&self,
		tenant: &str,
		key_id: &str,
		credential: Credential,
	) -> Result<CredentialMetadata> {
		let name = credential.name.clone();
		self.inner.audit.record(&AuditEvent::new(
			tenant,
			"api",
			"credential.put",
			name.as_str(),
			"attempted",
		))?;
		let metadata = match self.inner.credentials.put(tenant, key_id, credential) {
			Ok(metadata) => metadata,
			Err(error) => {
				self.inner.audit.record(&AuditEvent::new(
					tenant,
					"api",
					"credential.put",
					name.as_str(),
					"failed",
				))?;
				return Err(error);
			},
		};
		self.inner.audit.record(&AuditEvent::new(
			tenant,
			"api",
			"credential.put",
			name,
			"succeeded",
		))?;
		Ok(metadata)
	}

	fn credential_delete(&self, tenant: &str, name: &str) -> Result<()> {
		self.inner.audit.record(&AuditEvent::new(
			tenant,
			"api",
			"credential.delete",
			name,
			"attempted",
		))?;
		if let Err(error) = self.inner.credentials.delete(tenant, name) {
			self.inner.audit.record(&AuditEvent::new(
				tenant,
				"api",
				"credential.delete",
				name,
				"failed",
			))?;
			return Err(error);
		}
		self.inner.audit.record(&AuditEvent::new(
			tenant,
			"api",
			"credential.delete",
			name,
			"succeeded",
		))
	}

	fn info(&self) -> Result<Value> {
		Ok(json!({
			"version": env!("CARGO_PKG_VERSION"),
			"platform": std::env::consts::OS,
			"arch": std::env::consts::ARCH,
			"backend": backend_name(),
			"capabilities": {
				"snapshots": true,
				"fork": true,
				"exec": true,
				"files": true,
				"volumes": true,
				"pools": true,
				"user_net": cfg!(target_os = "macos"),
				"tap": net::has_net_admin(),
				"mesh": true,
			}
		}))
	}

	fn subscribe_events(&self) -> Receiver<Value> {
		let (tx, rx) = flume::unbounded();
		self.inner.events.lock().push(tx);
		rx
	}

	fn prometheus_metrics(&self) -> String {
		let records = self.inner.registry.list();
		let mut statuses = BTreeMap::<String, u64>::new();
		for record in records {
			*statuses.entry(record.status).or_default() += 1;
		}
		let counters = [
			("auth_failed", self.inner.counters.auth_failed.load(Ordering::Relaxed)),
			("created", self.inner.counters.created.load(Ordering::Relaxed)),
			("exec", self.inner.counters.exec.load(Ordering::Relaxed)),
			("file_delete", self.inner.counters.file_delete.load(Ordering::Relaxed)),
			("file_read", self.inner.counters.file_read.load(Ordering::Relaxed)),
			("file_write", self.inner.counters.file_write.load(Ordering::Relaxed)),
			("idle_reaped", self.inner.counters.idle_reaped.load(Ordering::Relaxed)),
			("snapshot", self.inner.counters.snapshot.load(Ordering::Relaxed)),
			("terminated", self.inner.counters.terminated.load(Ordering::Relaxed)),
		];
		let latency = self.inner.latency.lock();
		let (pool_hits, pool_misses) = self.inner.pools.total_hits_misses();
		let mut lines = vec![
			"# HELP vmon_server_sandboxes Number of sandboxes by status.".to_owned(),
			"# TYPE vmon_server_sandboxes gauge".to_owned(),
		];
		for (status, value) in statuses {
			lines.push(format!("vmon_server_sandboxes{{status=\"{status}\"}} {value}"));
		}
		lines.extend([
			"# HELP vmon_server_events_total Server supervisor events.".to_owned(),
			"# TYPE vmon_server_events_total counter".to_owned(),
		]);
		for (name, value) in counters {
			lines.push(format!("vmon_server_events_total{{event=\"{name}\"}} {value}"));
		}
		lines.extend([
			"# HELP vmon_server_create_latency_ms_sum Total sandbox create latency in milliseconds."
				.to_owned(),
			"# TYPE vmon_server_create_latency_ms_sum counter".to_owned(),
			format!("vmon_server_create_latency_ms_sum {}", latency.sum_ms),
			"# HELP vmon_server_create_latency_ms_count Count of observed sandbox creates.".to_owned(),
			"# TYPE vmon_server_create_latency_ms_count counter".to_owned(),
			format!("vmon_server_create_latency_ms_count {}", latency.count),
			"# HELP vmon_server_pool_hits Warm-pool claim hits observed by the server.".to_owned(),
			"# TYPE vmon_server_pool_hits counter".to_owned(),
			format!("vmon_server_pool_hits {pool_hits}"),
			"# HELP vmon_server_pool_misses Warm-pool claim misses observed by the server.".to_owned(),
			"# TYPE vmon_server_pool_misses counter".to_owned(),
			format!("vmon_server_pool_misses {pool_misses}"),
		]);
		if let Some((ingress, egress)) = net::host_network_bytes() {
			lines.extend([
				"# HELP vmon_server_network_bytes_total Cumulative bytes received or transmitted by \
				 the worker host on non-loopback network interfaces."
					.to_owned(),
				"# TYPE vmon_server_network_bytes_total counter".to_owned(),
				format!("vmon_server_network_bytes_total{{direction=\"ingress\"}} {ingress}"),
				format!("vmon_server_network_bytes_total{{direction=\"egress\"}} {egress}"),
			]);
		}
		format!("{}\n", lines.join("\n"))
	}

	fn migrate(&self, _id: &str, _target: &str) -> Result<Value> {
		Err(EngineError::unsupported("mesh migrate lands with the mesh port"))
	}
}

impl crate::engine::ExecControl for EngineExecControl {
	fn write_stdin(&mut self, data: &[u8]) -> Result<()> {
		self.handle.write_stdin(data)
	}

	fn close_stdin(&mut self) -> Result<()> {
		self.handle.close_stdin()
	}

	fn resize(&mut self, rows: u16, cols: u16) -> Result<()> {
		self.handle.resize(rows, cols)
	}

	fn kill(&mut self, signal: i32) -> Result<()> {
		self.handle.kill(signal)
	}
}
impl PtyControl for EnginePtyControl {
	fn write(&mut self, data: &[u8]) -> Result<()> {
		self.handle.write(data)
	}

	fn resize(&mut self, rows: u16, cols: u16) -> Result<()> {
		self.handle.resize(rows, cols)
	}

	fn detach(&mut self) -> Result<()> {
		self.handle.detach()
	}
}

impl Drop for EnginePtyControl {
	fn drop(&mut self) {
		let _ = self.handle.detach();
	}
}

fn align_process_home(home: &Path) {
	if crate::home::state_dir() == home {
		return;
	}
	// SAFETY: `Engine::new` is called during daemon startup before worker threads
	// are spawned; Wave-A path helpers intentionally read VMON_HOME process-wide.
	unsafe {
		std::env::set_var("VMON_HOME", home);
	}
}

fn validate_local_name(kind: &str, name: &str) -> Result<()> {
	if is_safe_snapshot_name(name) {
		Ok(())
	} else {
		Err(EngineError::invalid(format!(
			"{kind} must be a 1-128 byte ASCII basename using letters, digits, '.', '_', or '-'"
		)))
	}
}

fn read_snapshot_metadata(path: &Path) -> Result<Vec<u8>> {
	let file = fs::OpenOptions::new()
		.read(true)
		.custom_flags(libc::O_CLOEXEC | libc::O_NOFOLLOW)
		.open(path)
		.map_err(|error| {
			EngineError::invalid(format!("opening snapshot metadata {}: {error}", path.display()))
		})?;
	if !file
		.metadata()
		.map_err(|error| {
			EngineError::invalid(format!("stat snapshot metadata {}: {error}", path.display()))
		})?
		.is_file()
	{
		return Err(EngineError::invalid(format!(
			"snapshot metadata {} is not a regular file",
			path.display()
		)));
	}
	let mut bytes = Vec::new();
	Read::take(file, MAX_SNAPSHOT_METADATA_BYTES + 1).read_to_end(&mut bytes)?;
	if bytes.len() as u64 > MAX_SNAPSHOT_METADATA_BYTES {
		return Err(EngineError::invalid(format!(
			"snapshot metadata {} exceeds {MAX_SNAPSHOT_METADATA_BYTES} bytes",
			path.display()
		)));
	}
	Ok(bytes)
}

fn require_positive(name: &str, value: u64) -> Result<()> {
	if value == 0 {
		Err(EngineError::invalid(format!("{name} must be positive")))
	} else {
		Ok(())
	}
}

fn validate_ports(ports: Option<&[u16]>) -> Result<()> {
	for port in ports.unwrap_or(&[]) {
		if *port == 0 {
			return Err(EngineError::invalid("ports must be TCP port numbers from 1 to 65535"));
		}
	}
	Ok(())
}

fn validate_cidrs(name: &str, cidrs: Option<&[String]>) -> Result<()> {
	for cidr in cidrs.unwrap_or(&[]) {
		let (addr, prefix) = cidr
			.split_once('/')
			.map_or((cidr.as_str(), None), |(addr, prefix)| (addr, Some(prefix)));
		let ip = addr.parse::<IpAddr>().map_err(|_| {
			EngineError::invalid(format!("{name} entries must be valid CIDR networks"))
		})?;
		if let Some(prefix) = prefix {
			let prefix = prefix.parse::<u8>().map_err(|_| {
				EngineError::invalid(format!("{name} entries must be valid CIDR networks"))
			})?;
			let max = if ip.is_ipv4() { 32 } else { 128 };
			if prefix > max {
				return Err(EngineError::invalid(format!(
					"{name} entries must be valid CIDR networks"
				)));
			}
		}
	}
	Ok(())
}

fn validate_domains(name: &str, domains: Option<&[String]>) -> Result<()> {
	for domain in domains.unwrap_or(&[]) {
		if domain.trim().is_empty() {
			return Err(EngineError::invalid(format!("{name} entries must be non-empty")));
		}
	}
	Ok(())
}

fn validate_ha(value: Option<&str>) -> Result<()> {
	if let Some(value) = value
		&& !ALLOWED_HA.contains(&value)
	{
		return Err(EngineError::invalid(format!("ha must be one of: {}", ALLOWED_HA.join(", "))));
	}
	Ok(())
}

fn validate_arch(value: Option<&str>) -> Result<()> {
	if let Some(value) = value
		&& !matches!(value, "aarch64" | "x86_64")
	{
		return Err(EngineError::invalid("arch must be one of: aarch64, x86_64"));
	}
	Ok(())
}

fn restart_policy_for_ha(ha: &str) -> &'static str {
	if ha.contains("rerun") {
		"rerun"
	} else {
		"none"
	}
}

fn effective_timeout_secs(timeout_secs: Option<u64>, timeout: Option<f64>) -> Result<Option<u64>> {
	if timeout_secs == Some(0) {
		return Ok(None);
	}
	if let Some(timeout_secs) = timeout_secs {
		return Ok(Some(timeout_secs));
	}
	let secs = timeout.unwrap_or(DEFAULT_CREATE_TIMEOUT_SECS as f64);
	if !secs.is_finite() || secs < 0.0 {
		return Err(EngineError::invalid("timeout must be non-negative"));
	}
	Ok(Some(secs as u64))
}

fn resolve_snapshot_options(
	extra: HashMap<String, Value>,
	agent: Option<bool>,
) -> Result<ResolvedSnapshotOptions> {
	let options = serde_json::from_value::<SnapshotOptions>(json!(extra)).map_err(|error| {
		EngineError::invalid(format!("unsupported or invalid snapshot override: {error}"))
	})?;
	if options
		.persistence
		.as_ref()
		.and_then(PersistencePolicy::sticky_priority)
		.is_some_and(|priority| priority > 10)
	{
		return Err(EngineError::invalid("sticky persistence priority must be between 0 and 10"));
	}
	if options
		.idle_timeout_secs
		.is_some_and(|timeout| !timeout.is_finite() || timeout < 0.0)
	{
		return Err(EngineError::invalid("idle_timeout_secs must be non-negative"));
	}
	let secrets = parse_secrets(options.secrets)?;
	let timeout_secs = if options.timeout_secs.is_some() || options.timeout.is_some() {
		effective_timeout_secs(options.timeout_secs, options.timeout)?
	} else {
		None
	};
	Ok(ResolvedSnapshotOptions {
		agent: agent.or(options.agent).unwrap_or(false),
		block_network: options.block_network,
		env: options.env.unwrap_or_default().into_iter().collect(),
		secret_env: merge_secret_env(&secrets),
		secret_names: secrets.into_iter().map(|secret| secret.name).collect(),
		workdir: options.workdir,
		tags: options.tags.unwrap_or_default(),
		timeout_secs,
		idle_timeout_secs: options.idle_timeout_secs,
		activity_threshold_bytes: options.activity_threshold_bytes,
		persistence: options.persistence.unwrap_or_default(),
		readiness_probe: options.readiness_probe,
		s3_mounts: options.s3_mounts,
		command: options.command,
		credentials: options.credentials.unwrap_or_default(),
		owner_tenant: options.owner_tenant.unwrap_or_else(|| "default".to_owned()),
		encryption_key_id: options
			.encryption_key_id
			.unwrap_or_else(|| "default".to_owned()),
		ports: options.ports,
		egress_allow: options.egress_allow,
		egress_allow_domains: options.egress_allow_domains,
		inbound_cidr_allowlist: options.inbound_cidr_allowlist,
	})
}

const VOLUME_STAGING_MANIFEST: &str = "volume.json";

fn encrypted_volume_archive(home: &Home, name: &str) -> PathBuf {
	home
		.security_dir()
		.join("volumes")
		.join(format!("{name}.venc"))
}

fn encrypted_volume_runtime_root(home: &Home) -> PathBuf {
	home.security_dir().join("runtime").join("volumes")
}

fn create_private_dir(path: &Path) -> Result<()> {
	fs::create_dir_all(path)?;
	let metadata = fs::symlink_metadata(path)?;
	if metadata.file_type().is_symlink() || !metadata.is_dir() {
		return Err(EngineError::invalid(format!(
			"private runtime path {} is not a directory",
			path.display()
		)));
	}
	fs::set_permissions(path, fs::Permissions::from_mode(0o700))?;
	Ok(())
}

fn write_volume_staging_manifest(
	slot_dir: &Path,
	name: &str,
	key_id: &str,
	read_only: bool,
) -> Result<()> {
	let manifest = serde_json::to_vec(&json!({
		"version": 1,
		"name": name,
		"key_id": key_id,
		"read_only": read_only,
	}))?;
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o600)
		.open(slot_dir.join(VOLUME_STAGING_MANIFEST))?;
	file.write_all(&manifest)?;
	file.sync_all()?;
	Ok(())
}

fn load_staged_volume_mounts(home: &Home, sid: &str) -> Result<Vec<EncryptedVolumeMount>> {
	let root = encrypted_volume_runtime_root(home).join(sid);
	let metadata = match fs::symlink_metadata(&root) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(Vec::new()),
		Err(error) => return Err(error.into()),
	};
	if metadata.file_type().is_symlink() || !metadata.is_dir() {
		return Err(EngineError::invalid(format!(
			"encrypted volume runtime path {} is not a directory",
			root.display()
		)));
	}
	let mut slots = fs::read_dir(&root)?.collect::<io::Result<Vec<_>>>()?;
	slots.sort_by_key(fs::DirEntry::file_name);
	let mut mounts = Vec::with_capacity(slots.len());
	for slot in slots {
		let slot_dir = slot.path();
		let metadata = slot.file_type()?;
		if metadata.is_symlink() || !metadata.is_dir() {
			return Err(EngineError::invalid(format!(
				"encrypted volume staging entry {} is not a directory",
				slot_dir.display()
			)));
		}
		let manifest_path = slot_dir.join(VOLUME_STAGING_MANIFEST);
		let manifest = match fs::read(&manifest_path) {
			Ok(manifest) => manifest,
			Err(error) if error.kind() == io::ErrorKind::NotFound => {
				fs::remove_dir_all(&slot_dir)?;
				continue;
			},
			Err(error) => return Err(error.into()),
		};
		if manifest.len() > 4096 {
			return Err(EngineError::invalid(format!(
				"encrypted volume staging manifest {} is too large",
				manifest_path.display()
			)));
		}
		let value: Value = serde_json::from_slice(&manifest)?;
		if value.get("version").and_then(Value::as_u64) != Some(1) {
			return Err(EngineError::invalid("unsupported encrypted volume staging manifest"));
		}
		let name = value.get("name").and_then(Value::as_str).ok_or_else(|| {
			EngineError::invalid("encrypted volume staging manifest is missing name")
		})?;
		let volume = Volume::new_in_home(home.root(), name)?;
		if slot.file_name() != std::ffi::OsStr::new(name) {
			return Err(EngineError::invalid(format!(
				"encrypted volume staging directory {} does not match manifest name {name:?}",
				slot_dir.display()
			)));
		}
		let key_id = value
			.get("key_id")
			.and_then(Value::as_str)
			.filter(|key| !key.is_empty())
			.ok_or_else(|| {
				EngineError::invalid("encrypted volume staging manifest is missing key_id")
			})?;
		let read_only = value
			.get("read_only")
			.and_then(Value::as_bool)
			.unwrap_or(false);
		let mount_dir = slot_dir.join(name);
		let mount_metadata = fs::symlink_metadata(&mount_dir)?;
		if mount_metadata.file_type().is_symlink() || !mount_metadata.is_dir() {
			return Err(EngineError::invalid(format!(
				"encrypted volume mount {} is not a directory",
				mount_dir.display()
			)));
		}
		mounts.push(EncryptedVolumeMount {
			name: name.to_owned(),
			mount_dir,
			slot_dir,
			archive: encrypted_volume_archive(home, volume.name()),
			key_id: key_id.to_owned(),
			read_only,
			sealed: read_only,
			preserve: false,
		});
	}
	Ok(mounts)
}

fn volume_has_plaintext(path: &Path) -> Result<bool> {
	for entry in fs::read_dir(path)? {
		if entry?.file_name() != std::ffi::OsStr::new(".lock") {
			return Ok(true);
		}
	}
	Ok(false)
}

fn remove_legacy_volume_data(path: &Path) -> Result<()> {
	for entry in fs::read_dir(path)? {
		let entry = entry?;
		if entry.file_name() == std::ffi::OsStr::new(".lock") {
			continue;
		}
		let target = entry.path();
		if entry.file_type()?.is_dir() {
			fs::remove_dir_all(target)?;
		} else {
			fs::remove_file(target)?;
		}
	}
	Ok(())
}

fn parse_volume_spec(value: &Value) -> Result<(String, bool)> {
	if let Some(name) = value.as_str() {
		return Ok((name.to_owned(), false));
	}
	let object = value
		.as_object()
		.ok_or_else(|| EngineError::invalid("volume spec must be a string or object"))?;
	let name = object
		.get("name")
		.and_then(Value::as_str)
		.filter(|name| !name.is_empty())
		.ok_or_else(|| EngineError::invalid("volume object requires a name"))?;
	Ok((
		name.to_owned(),
		object
			.get("read_only")
			.and_then(Value::as_bool)
			.unwrap_or(false),
	))
}

fn s3_credentials(mountpoint: &str, spec: &S3MountSpec) -> Result<(Option<S3Credentials>, S3Auth)> {
	let inline_requested =
		spec.access_key.is_some() || spec.secret_key.is_some() || spec.session_token.is_some();
	let access_key = spec.access_key.as_deref().filter(|value| !value.is_empty());
	let secret_key = spec.secret_key.as_deref().filter(|value| !value.is_empty());
	if let (Some(access_key), Some(secret_key)) = (access_key, secret_key) {
		return Ok((
			Some(S3Credentials {
				access_key:    access_key.to_owned(),
				secret_key:    secret_key.to_owned(),
				session_token: spec.session_token.clone().filter(|value| !value.is_empty()),
			}),
			S3Auth::Inline,
		));
	}
	if inline_requested {
		return Err(EngineError::invalid(format!(
			"S3 mount {mountpoint} requires both access_key and secret_key"
		)));
	}
	if let Some(creds) = environment_s3_credentials() {
		return Ok((Some(creds), S3Auth::Env));
	}
	Ok((None, S3Auth::Anonymous))
}

fn environment_s3_credentials() -> Option<S3Credentials> {
	let access_key = std::env::var("AWS_ACCESS_KEY_ID")
		.ok()
		.filter(|value| !value.is_empty())?;
	let secret_key = std::env::var("AWS_SECRET_ACCESS_KEY")
		.ok()
		.filter(|value| !value.is_empty())?;
	Some(S3Credentials {
		access_key,
		secret_key,
		session_token: std::env::var("AWS_SESSION_TOKEN")
			.ok()
			.filter(|value| !value.is_empty()),
	})
}

fn apply_restored_s3_tags(
	mounts: &mut [ResolvedS3Mount],
	volumes: &[ResolvedVolume],
	tags: &HashMap<String, String>,
) -> Result<()> {
	if mounts.len() != tags.len() {
		return Err(EngineError::invalid(
			"restored S3 mount metadata does not match the requested mounts",
		));
	}
	let mut used = volumes
		.iter()
		.map(|volume| volume.tag.clone())
		.collect::<HashSet<_>>();
	for mount in mounts {
		let tag = tags.get(&mount.mountpoint).ok_or_else(|| {
			EngineError::invalid(format!(
				"restored S3 mount metadata is missing tag for {}",
				mount.mountpoint
			))
		})?;
		if !valid_virtiofs_tag(tag) {
			return Err(EngineError::invalid(format!(
				"restored S3 mount has invalid virtio-fs tag {tag:?}"
			)));
		}
		if !used.insert(tag.clone()) {
			return Err(EngineError::invalid(format!(
				"restored S3 mount tag {tag:?} collides with another filesystem mount"
			)));
		}
		mount.tag.clone_from(tag);
		mount
			.meta
			.as_object_mut()
			.expect("S3 mount metadata is always an object")
			.insert("tag".to_owned(), json!(tag));
	}
	Ok(())
}

fn valid_virtiofs_tag(tag: &str) -> bool {
	!tag.is_empty()
		&& tag.len() <= 32
		&& tag
			.bytes()
			.all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
}

fn restore_s3_mount_params(
	params: &mut Map<String, Value>,
) -> Result<Option<HashMap<String, String>>> {
	let Some(source) = params
		.get("s3_mounts")
		.filter(|mounts| !mounts.is_null())
		.cloned()
	else {
		return Ok(None);
	};
	let mounts = source
		.as_object()
		.ok_or_else(|| EngineError::invalid("restored S3 mounts must be an object"))?;
	let has_metadata = mounts.values().any(|mount| mount.get("auth").is_some());
	if !has_metadata {
		return Ok(None);
	}
	let mut restored = Map::new();
	let mut tags = HashMap::with_capacity(mounts.len());
	for (mountpoint, mount) in mounts {
		let object = mount.as_object().ok_or_else(|| {
			EngineError::invalid(format!("restored S3 mount {mountpoint} must be an object"))
		})?;
		let uri = object
			.get("uri")
			.and_then(Value::as_str)
			.filter(|uri| !uri.is_empty())
			.ok_or_else(|| {
				EngineError::invalid(format!("restored S3 mount {mountpoint} is missing uri"))
			})?;
		let tag = object
			.get("tag")
			.and_then(Value::as_str)
			.filter(|tag| !tag.is_empty())
			.ok_or_else(|| {
				EngineError::invalid(format!("restored S3 mount {mountpoint} is missing tag"))
			})?;
		let auth = object.get("auth").and_then(Value::as_str).ok_or_else(|| {
			EngineError::invalid(format!("restored S3 mount {mountpoint} is missing auth"))
		})?;
		match auth {
			"inline" => {
				if environment_s3_credentials().is_none() {
					return Err(EngineError::invalid(format!(
						"S3 mount {mountpoint} was created with inline credentials; set \
						 AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to restore"
					)));
				}
			},
			"env" => {
				if environment_s3_credentials().is_none() {
					return Err(EngineError::invalid(format!(
						"S3 mount {mountpoint} requires AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY to \
						 restore"
					)));
				}
			},
			"anonymous" => {},
			_ => {
				return Err(EngineError::invalid(format!(
					"restored S3 mount {mountpoint} has unknown auth mode {auth:?}"
				)));
			},
		}
		let spec = Map::from_iter([
			("uri".to_owned(), json!(uri)),
			("endpoint".to_owned(), object.get("endpoint").cloned().unwrap_or(Value::Null)),
			("region".to_owned(), object.get("region").cloned().unwrap_or(Value::Null)),
			(
				"read_only".to_owned(),
				json!(
					object
						.get("read_only")
						.and_then(Value::as_bool)
						.unwrap_or(false)
				),
			),
		]);
		if let Some(endpoint) = spec.get("endpoint")
			&& !endpoint.is_null()
			&& !endpoint.is_string()
		{
			return Err(EngineError::invalid(format!(
				"restored S3 mount {mountpoint} has invalid endpoint"
			)));
		}
		if let Some(region) = spec.get("region")
			&& !region.is_null()
			&& !region.is_string()
		{
			return Err(EngineError::invalid(format!(
				"restored S3 mount {mountpoint} has invalid region"
			)));
		}
		restored.insert(mountpoint.clone(), Value::Object(spec));
		tags.insert(mountpoint.clone(), tag.to_owned());
	}
	params.insert("s3_mounts".to_owned(), Value::Object(restored));
	Ok(Some(tags))
}

fn unique_volume_tag(base: &str, used: &mut std::collections::HashSet<String>) -> String {
	let mut stem = base
		.to_ascii_lowercase()
		.chars()
		.map(|ch| {
			if ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_' {
				ch
			} else {
				'_'
			}
		})
		.take(32)
		.collect::<String>();
	if stem.is_empty() {
		"vol".clone_into(&mut stem);
	}
	let mut candidate = stem.clone();
	let mut suffix = 2;
	while used.contains(&candidate) {
		let tail = format!("_{suffix}");
		candidate = format!("{}{}", &stem[..stem.len().min(32 - tail.len())], tail);
		suffix += 1;
	}
	used.insert(candidate.clone());
	candidate
}

fn parse_secrets(secrets: Option<Vec<Value>>) -> Result<Vec<Secret>> {
	secrets
		.unwrap_or_default()
		.into_iter()
		.map(Secret::from_wire)
		.collect::<Result<Vec<_>>>()
}

fn merge_secret_env(secrets: &[Secret]) -> BTreeMap<String, String> {
	let mut env = BTreeMap::new();
	for secret in secrets {
		for (key, value) in secret.env_pairs() {
			env.insert(key, value);
		}
	}
	env
}

fn image_env(
	image_spec: Option<&image::ImageConfig>,
	overrides: Option<&HashMap<String, String>>,
) -> BTreeMap<String, String> {
	let mut env = image_spec
		.map(image::ImageConfig::env_dict)
		.unwrap_or_default()
		.into_iter()
		.collect::<BTreeMap<_, _>>();
	if let Some(overrides) = overrides {
		for (key, value) in overrides {
			env.insert(key.clone(), value.clone());
		}
	}
	env
}

fn merged_env(
	state: &RuntimeState,
	overrides: Option<&HashMap<String, String>>,
) -> BTreeMap<String, String> {
	let mut env = state.env.clone();
	env.extend(state.secret_env.clone());
	if let Some(overrides) = overrides {
		for (key, value) in overrides {
			env.insert(key.clone(), value.clone());
		}
	}
	env
}

fn readiness_argv(probe: &Value) -> Vec<String> {
	if let Some(text) = probe.as_str() {
		return vec!["sh".to_owned(), "-lc".to_owned(), text.to_owned()];
	}
	probe.as_array().map_or_else(
		|| vec![probe.to_string()],
		|items| {
			items
				.iter()
				.map(|item| {
					item
						.as_str()
						.map_or_else(|| item.to_string(), ToOwned::to_owned)
				})
				.collect()
		},
	)
}

fn stamp_checkpoint_rootfs(
	home: &Home,
	snapshot_dir: &Path,
	detail: Option<&Map<String, Value>>,
) -> Result<()> {
	let rootfs = snapshot_dir.join("rootfs.img");
	if rootfs.is_file() {
		return Ok(());
	}
	let mut tried = Vec::new();
	if let Some(detail) = detail {
		for candidate in ["rootfs", "template", "restored_from"] {
			let Some(value) = detail
				.get(candidate)
				.and_then(Value::as_str)
				.filter(|value| !value.is_empty())
			else {
				continue;
			};
			for source in checkpoint_rootfs_candidates(home, value) {
				let src_rootfs = if source.is_dir() {
					source.join("rootfs.img")
				} else {
					source.clone()
				};
				tried.push(src_rootfs.clone());
				if src_rootfs.is_file() {
					fs::copy(src_rootfs, &rootfs)?;
					return Ok(());
				}
			}
		}
	}
	Err(EngineError::engine(format!(
		"mesh checkpoint {} has no rootfs.img; tried {}",
		snapshot_dir.display(),
		tried
			.iter()
			.map(|path| path.display().to_string())
			.collect::<Vec<_>>()
			.join(", ")
	)))
}

fn checkpoint_rootfs_candidates(home: &Home, value: &str) -> Vec<PathBuf> {
	let source = PathBuf::from(value);
	if source.is_absolute() {
		return vec![source];
	}
	vec![source, home.templates_dir().join(value), home.root().join(value)]
}

fn stamp_checkpoint_marker(snapshot_dir: &Path, detail: Option<&Map<String, Value>>) -> Result<()> {
	let marker = snapshot_dir.join("agent-ready.json");
	if marker.is_file() {
		return Ok(());
	}
	let Some(detail) = detail else {
		return Ok(());
	};
	for candidate in ["rootfs", "template", "restored_from"] {
		let Some(value) = detail
			.get(candidate)
			.and_then(Value::as_str)
			.filter(|value| !value.is_empty())
		else {
			continue;
		};
		let source = PathBuf::from(value);
		let source_dir = if candidate == "rootfs" {
			source.parent().unwrap_or(source.as_path())
		} else {
			source.as_path()
		};
		if copy_agent_marker(source_dir, snapshot_dir)? {
			break;
		}
	}
	Ok(())
}

fn copy_agent_marker(source_dir: &Path, target_dir: &Path) -> Result<bool> {
	let source = source_dir.join("agent-ready.json");
	if !source.is_file() {
		return Ok(false);
	}
	let target = target_dir.join("agent-ready.json");
	if source != target {
		fs::copy(source, target)?;
	}
	Ok(true)
}

fn ensure_checkpoint_template_present(snapshot_dir: &Path) -> Result<()> {
	let rootfs = snapshot_dir.join("rootfs.img");
	let marker = snapshot_dir.join("agent-ready.json");
	if rootfs.is_file() && marker.is_file() {
		return Ok(());
	}
	Err(EngineError::engine(format!(
		"mesh checkpoint {} is not bootable; rootfs={} marker={}",
		snapshot_dir.display(),
		rootfs.is_file(),
		marker.is_file(),
	)))
}

/// Writable volume names described by a registry record's detail.
fn writable_volume_names_from_detail(detail: &Value) -> Vec<String> {
	detail.as_object().map_or_else(Vec::new, |params| {
		crate::mesh::runtime::writable_volumes(params).unwrap_or_default()
	})
}

fn capture_checkpoint_volumes(
	engine: &Engine,
	sid: &str,
	snapshot_dir: &Path,
	volumes: Option<&Value>,
) -> Result<()> {
	let Some(Value::Object(volumes)) = volumes else {
		return Ok(());
	};
	let dest_root = snapshot_dir.join("volumes");
	fs::create_dir(&dest_root)?;
	let mut seen = HashSet::new();
	for spec in volumes.values() {
		let Some(name) = spec
			.get("name")
			.and_then(Value::as_str)
			.filter(|name| !name.is_empty())
		else {
			continue;
		};
		if !seen.insert(name.to_owned()) {
			continue;
		}
		let active = engine.inner.runtimes.lock().get(sid).and_then(|runtime| {
			runtime
				.encrypted_volumes
				.iter()
				.find(|volume| volume.name == name)
				.map(|volume| volume.mount_dir.clone())
		});
		let mut guard = None;
		let source = if let Some(active) = active {
			active
		} else {
			let archive = encrypted_volume_archive(engine.home(), name);
			if !archive.is_file() {
				return Err(EngineError::unsupported(format!(
					"volume {name:?} has no encrypted archive at {}",
					archive.display()
				)));
			}
			let extract_root = encrypted_volume_runtime_root(engine.home())
				.join(format!(".capture-{}", random_hex(16)));
			let source = EncryptedArchive::open(&archive, &extract_root, &engine.inner.keyring)?;
			guard = Some(TransientDir(extract_root));
			source
		};
		copy_tree_without_locks(&source, &dest_root.join(name))?;
		drop(guard);
	}
	Ok(())
}

struct VolumeArchiveBackup {
	archive: PathBuf,
	backup:  Option<PathBuf>,
}

impl VolumeArchiveBackup {
	fn commit(self) {
		if let Some(backup) = self.backup {
			let _ = fs::remove_file(backup);
		}
	}

	fn rollback(self) -> Result<()> {
		match self.backup {
			Some(backup) => fs::rename(backup, self.archive)?,
			None => match fs::remove_file(self.archive) {
				Ok(()) => {},
				Err(error) if error.kind() == io::ErrorKind::NotFound => {},
				Err(error) => return Err(error.into()),
			},
		}
		Ok(())
	}
}

fn remove_volume_artifact(path: &Path) {
	if path.is_dir() {
		let _ = fs::remove_dir_all(path);
	} else {
		let _ = fs::remove_file(path);
	}
}

/// Restore checkpoint-carried volume data onto this node before create.
///
/// Returns the freshly-created host volume directories (for rollback on a
/// later create failure). Refuses to clobber a pre-existing same-named local
/// volume (there is no cluster-unique volume identity yet) and refuses a
/// checkpoint that is missing data for a declared volume. Every volume is
/// validated BEFORE any copy so a rejection leaves no partial state behind.
fn materialize_checkpoint_volumes(
	home: &Home,
	keyring: &Keyring,
	key_id: &str,
	snapshot_dir: &Path,
	volumes: Option<&Map<String, Value>>,
) -> Result<Vec<PathBuf>> {
	drop(keyring.load(key_id)?);
	let Some(volumes) = volumes.filter(|volumes| !volumes.is_empty()) else {
		return Ok(Vec::new());
	};
	let source_root = snapshot_dir.join("volumes");
	let mut names = Vec::new();
	let mut seen = HashSet::new();
	for spec in volumes.values() {
		let Some(name) = spec
			.get("name")
			.and_then(Value::as_str)
			.filter(|name| !name.is_empty())
		else {
			continue;
		};
		if seen.insert(name.to_owned()) {
			names.push(name.to_owned());
		}
	}
	for name in &names {
		let volume_dir = home.volumes_dir().join(name);
		let archive = encrypted_volume_archive(home, name);
		if volume_dir.exists() || volume_dir.is_symlink() || archive.exists() || archive.is_symlink()
		{
			return Err(EngineError::unsupported(format!(
				"cannot restore volume {name:?}: a volume of that name already exists on this node \
				 (no cluster-unique volume identity yet)"
			)));
		}
		let source = source_root.join(name);
		let metadata = fs::symlink_metadata(&source).map_err(|_| {
			EngineError::unsupported(format!("checkpoint is missing data for volume {name:?}"))
		})?;
		if metadata.file_type().is_symlink() || !metadata.is_dir() {
			return Err(EngineError::invalid(format!(
				"checkpoint volume {name:?} is not a regular directory"
			)));
		}
	}
	let mut created = Vec::new();
	for name in &names {
		let result = (|| {
			let volume = Volume::new_in_home(home.root(), name)?;
			created.push(volume.path());
			let archive = encrypted_volume_archive(home, name);
			EncryptedArchive::seal(&source_root.join(name), &archive, keyring, key_id)?;
			created.push(archive);
			Ok(())
		})();
		if let Err(error) = result {
			for path in &created {
				remove_volume_artifact(path);
			}
			return Err(error);
		}
	}
	Ok(created)
}

fn restore_checkpoint_volumes_in_place(
	home: &Home,
	keyring: &Keyring,
	key_id: &str,
	snapshot_dir: &Path,
	volumes: Option<&Map<String, Value>>,
) -> Result<Vec<VolumeArchiveBackup>> {
	drop(keyring.load(key_id)?);
	let Some(volumes) = volumes.filter(|volumes| !volumes.is_empty()) else {
		return Ok(Vec::new());
	};
	let source_root = snapshot_dir.join("volumes");
	let mut names = Vec::new();
	let mut seen = HashSet::new();
	for spec in volumes.values() {
		let Some(name) = spec
			.get("name")
			.and_then(Value::as_str)
			.filter(|name| !name.is_empty())
		else {
			continue;
		};
		validate_local_name("volume name", name)?;
		if seen.insert(name.to_owned()) {
			let source = source_root.join(name);
			let metadata = fs::symlink_metadata(&source).map_err(|_| {
				EngineError::unsupported(format!("checkpoint is missing data for volume {name:?}"))
			})?;
			if metadata.file_type().is_symlink() || !metadata.is_dir() {
				return Err(EngineError::invalid(format!(
					"checkpoint volume {name:?} is not a regular directory"
				)));
			}
			names.push(name.to_owned());
		}
	}
	let mut backups = Vec::new();
	for name in names {
		let result = (|| {
			let volume = Volume::new_in_home(home.root(), &name)?;
			let _lock = volume.acquire_write_lock()?;
			let archive = encrypted_volume_archive(home, &name);
			let backup = if archive.exists() || archive.is_symlink() {
				let metadata = fs::symlink_metadata(&archive)?;
				if metadata.file_type().is_symlink() || !metadata.is_file() {
					return Err(EngineError::invalid(format!(
						"encrypted volume archive {} is not a regular file",
						archive.display()
					)));
				}
				let backup = archive.with_file_name(format!(".{name}.{}.backup", random_hex(12)));
				fs::copy(&archive, &backup)?;
				fs::set_permissions(&backup, fs::Permissions::from_mode(0o600))?;
				Some(backup)
			} else {
				None
			};
			backups.push(VolumeArchiveBackup { archive: archive.clone(), backup });
			EncryptedArchive::seal(&source_root.join(&name), &archive, keyring, key_id)
		})();
		if let Err(error) = result {
			for backup in backups.into_iter().rev() {
				backup.rollback()?;
			}
			return Err(error);
		}
	}
	Ok(backups)
}

/// Refuse cross-platform network restores. Port of Python
/// `Engine._validate_network_restore`: a checkpoint that carried a `network`
/// restore spec can only resume on a host with the same network flavor.
fn validate_network_restore(params: &Map<String, Value>) -> Result<()> {
	let Some(Value::Object(network)) = params.get("network") else {
		return Ok(());
	};
	let flavor = network
		.get("flavor")
		.and_then(Value::as_str)
		.unwrap_or_default();
	let macos = cfg!(target_os = "macos");
	match flavor {
		"tap" if macos => Err(EngineError::unsupported(
			"Linux TAP networking cannot be restored on macOS user-net hosts",
		)),
		"user" if !macos => Err(EngineError::unsupported(
			"macOS user-net checkpoints cannot be restored on Linux TAP hosts",
		)),
		"tap" | "user" => Ok(()),
		other => {
			Err(EngineError::unsupported(format!("unknown network checkpoint flavor '{other}'")))
		},
	}
}

fn require_regular_file(path: &Path, label: &str) -> Result<()> {
	let metadata = fs::symlink_metadata(path)?;
	if metadata.file_type().is_symlink() || !metadata.is_file() {
		return Err(EngineError::invalid(format!(
			"{label} {} must be a regular file",
			path.display()
		)));
	}
	Ok(())
}

fn move_or_copy_regular_file(source: &Path, destination: &Path) -> Result<()> {
	require_regular_file(source, "capture source")?;
	match fs::rename(source, destination) {
		Ok(()) => Ok(()),
		Err(error) if error.raw_os_error() == Some(libc::EXDEV) => {
			fs::copy(source, destination)?;
			Ok(())
		},
		Err(error) => Err(error.into()),
	}
}

fn reclaim_orphaned_disk_artifacts(home: &Home) -> Result<()> {
	let root = home.root().join("snapshots");
	let entries = match fs::read_dir(&root) {
		Ok(entries) => entries,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(error) => return Err(error.into()),
	};
	for entry in entries {
		let entry = entry?;
		let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
			continue;
		};
		if !is_safe_snapshot_name(&name) {
			continue;
		}
		let point = entry.path();
		let metadata = fs::symlink_metadata(&point)?;
		if metadata.file_type().is_symlink() || !metadata.is_dir() {
			continue;
		}

		let mut removable = Vec::new();
		let mut managed_only = true;
		for child in fs::read_dir(&point)? {
			let child = child?;
			let child_name = child.file_name();
			let child_name = child_name.to_string_lossy();
			let child_path = child.path();
			if (child_name == "disk" && is_published_disk_artifact(&child_path))
				|| (is_disk_staging_name(&child_name) && is_regular_directory(&child_path)?)
			{
				removable.push(child_path);
			} else {
				managed_only = false;
				break;
			}
		}
		if managed_only {
			for path in removable {
				fs::remove_dir_all(path)?;
			}
		}
	}
	Ok(())
}

fn is_regular_directory(path: &Path) -> Result<bool> {
	let metadata = fs::symlink_metadata(path)?;
	Ok(!metadata.file_type().is_symlink() && metadata.is_dir())
}

fn is_disk_staging_name(name: &str) -> bool {
	let Some(stem) = name
		.strip_prefix(".disk.")
		.and_then(|name| name.strip_suffix(".tmp"))
	else {
		return false;
	};
	let mut parts = stem.split('.');
	matches!(
		(parts.next(), parts.next(), parts.next()),
		(Some(pid), Some(sequence), None)
			if !pid.is_empty()
				&& !sequence.is_empty()
				&& pid.bytes().all(|byte| byte.is_ascii_digit())
				&& sequence.bytes().all(|byte| byte.is_ascii_digit())
	)
}

fn is_published_disk_artifact(path: &Path) -> bool {
	if !is_regular_directory(path).unwrap_or(false) {
		return false;
	}
	let Ok(mut entries) = fs::read_dir(path) else {
		return false;
	};
	let Some(Ok(first)) = entries.next() else {
		return false;
	};
	let Some(Ok(second)) = entries.next() else {
		return false;
	};
	if entries.next().is_some() {
		return false;
	}
	let paths = [first.path(), second.path()];
	let rootfs = path.join("rootfs.img");
	let manifest = path.join("manifest.json");
	if !paths.contains(&rootfs)
		|| !paths.contains(&manifest)
		|| require_regular_file(&rootfs, "disk rootfs").is_err()
		|| require_regular_file(&manifest, "disk manifest").is_err()
	{
		return false;
	}
	let Ok(bytes) = fs::read(&manifest) else {
		return false;
	};
	serde_json::from_slice::<Value>(&bytes).is_ok_and(|manifest| {
		manifest.get("version").and_then(Value::as_u64) == Some(1)
			&& manifest.get("rootfs").and_then(Value::as_str) == Some("rootfs.img")
			&& manifest.get("boot").and_then(Value::as_str) == Some("cold")
	})
}

fn copy_checkpoint_tree_cow(source: &Path, destination: &Path) -> Result<()> {
	let source_metadata = fs::symlink_metadata(source)?;
	if source_metadata.file_type().is_symlink() || !source_metadata.is_dir() {
		return Err(EngineError::invalid(format!(
			"migration checkpoint {} must be a regular directory",
			source.display()
		)));
	}
	fs::create_dir(destination)?;
	for entry in fs::read_dir(source)? {
		let entry = entry?;
		let from = entry.path();
		let to = destination.join(entry.file_name());
		let metadata = fs::symlink_metadata(&from)?;
		if metadata.file_type().is_symlink() {
			return Err(EngineError::invalid("migration checkpoint contains symlink"));
		}
		if metadata.is_dir() {
			copy_checkpoint_tree_cow(&from, &to)?;
		} else if metadata.is_file() {
			drop(vmm::create_cow_overlay(&from, &to).map_err(|error| {
				EngineError::engine(format!(
					"staging migration file {} -> {}: {error}",
					from.display(),
					to.display()
				))
			})?);
		} else {
			return Err(EngineError::invalid("migration checkpoint contains unsupported entry"));
		}
	}
	Ok(())
}

fn copy_tree_without_locks(src: &Path, dst: &Path) -> Result<()> {
	let source_metadata = fs::symlink_metadata(src)?;
	if source_metadata.file_type().is_symlink() || !source_metadata.is_dir() {
		return Err(EngineError::invalid(format!(
			"volume copy source {} must be a regular directory",
			src.display()
		)));
	}
	match fs::symlink_metadata(dst) {
		Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_dir() => {
			return Err(EngineError::invalid(format!(
				"volume copy destination {} must be a regular directory",
				dst.display()
			)));
		},
		Ok(_) => {},
		Err(error) if error.kind() == io::ErrorKind::NotFound => fs::create_dir(dst)?,
		Err(error) => return Err(error.into()),
	}
	for entry in fs::read_dir(src)? {
		let entry = entry?;
		if entry.file_name() == ".lock" {
			continue;
		}
		let path = entry.path();
		let target = dst.join(entry.file_name());
		let file_type = entry.file_type()?;
		if file_type.is_dir() {
			copy_tree_without_locks(&path, &target)?;
		} else if file_type.is_symlink() {
			std::os::unix::fs::symlink(fs::read_link(&path)?, &target)?;
		} else if file_type.is_file() {
			fs::copy(&path, &target)?;
		} else {
			return Err(EngineError::unsupported(format!(
				"volume entry {} is not a file, directory, or symlink",
				path.display()
			)));
		}
	}
	Ok(())
}

fn volumes_meta(volumes: &[ResolvedVolume]) -> Value {
	Value::Object(
		volumes
			.iter()
			.map(|volume| {
				(
					volume.mountpoint.clone(),
					json!({ "name": volume.name, "tag": volume.tag, "read_only": volume.read_only }),
				)
			})
			.collect::<Map<_, _>>(),
	)
}

fn s3_mounts_meta(mounts: &[ResolvedS3Mount]) -> Value {
	Value::Object(
		mounts
			.iter()
			.map(|mount| (mount.mountpoint.clone(), mount.meta.clone()))
			.collect(),
	)
}

fn network_guest_json(config: &net::GuestNetworkConfig) -> Value {
	json!({
		"tap": config.tap,
		"prefix": config.prefix,
		"host_ip": config.host_ip,
		"dns": config.dns,
	})
}

fn user_net_guest_config() -> Value {
	json!({
		"guest_ip": net::USER_NET_GUEST_IP,
		"prefix": net::USER_NET_PREFIX,
		"host_ip": net::USER_NET_GATEWAY,
		"dns": net::USER_NET_DNS,
	})
}

fn policy_json(policy: &NetworkPolicy) -> Value {
	json!({
		"block_network": policy.block_network,
		"egress_allow": policy.egress_allow,
		"egress_allow_domains": policy.egress_allow_domains,
		"inbound_cidr_allowlist": policy.inbound_cidr_allowlist,
	})
}

/// Build the exact durable identity that may cross a daemon boundary. Runtime
/// capabilities and secret values are intentionally absent.
fn safe_runtime_identity(
	state: &RuntimeState,
	secret_names: impl IntoIterator<Item = String>,
	timeout_secs: Option<f64>,
	source: Option<String>,
	template: Option<String>,
) -> SafeRuntimeIdentity {
	let secret_names: std::collections::BTreeSet<String> = secret_names.into_iter().collect();
	let identity_complete = secret_names.is_empty();
	SafeRuntimeIdentity {
		version: 1,
		identity_complete,
		environment: state.env.clone(),
		workdir: state.workdir.clone(),
		network_policy: Some(policy_json(&state.network_policy)),
		network: state.network_spec.clone().unwrap_or(Value::Null),
		tunnels: state.network_spec.clone().unwrap_or(Value::Null),
		mounts: Vec::new(),
		timeout_secs,
		source,
		template,
		pool: None,
		available_secret_names: Default::default(),
		secret_names,
	}
}
fn runtime_identity(state: &RuntimeState) -> Value {
	serde_json::to_value(safe_runtime_identity(state, std::iter::empty(), None, None, None))
		.expect("safe runtime identity serializes")
}

fn string_list(value: Option<&Value>) -> Option<Vec<String>> {
	value.and_then(Value::as_array).map(|items| {
		items
			.iter()
			.filter_map(Value::as_str)
			.map(ToOwned::to_owned)
			.collect()
	})
}

fn runtime_state_from_safe_identity(identity: &SafeRuntimeIdentity) -> RuntimeState {
	let policy = identity.network_policy.as_ref().and_then(Value::as_object);
	RuntimeState {
		env: identity.environment.clone(),
		workdir: identity.workdir.clone(),
		network_policy: NetworkPolicy {
			block_network:          policy
				.and_then(|policy| policy.get("block_network"))
				.and_then(Value::as_bool),
			egress_allow:           string_list(policy.and_then(|policy| policy.get("egress_allow"))),
			egress_allow_domains:   string_list(
				policy.and_then(|policy| policy.get("egress_allow_domains")),
			),
			inbound_cidr_allowlist: string_list(
				policy.and_then(|policy| policy.get("inbound_cidr_allowlist")),
			),
		},
		network_spec: (!identity.tunnels.is_null()).then(|| identity.tunnels.clone()),
		identity_complete: identity.identity_complete && identity.secret_names.is_empty(),
		..RuntimeState::default()
	}
}

fn sorted_ports(ports: Option<&[u16]>, tunnels: &BTreeMap<u16, (String, u16)>) -> Vec<u16> {
	let mut ports =
		ports.map_or_else(|| tunnels.keys().copied().collect::<Vec<_>>(), <[u16]>::to_vec);
	ports.sort_unstable();
	ports
}

fn tunnels_json(tunnels: &BTreeMap<u16, (String, u16)>) -> Value {
	Value::Object(
		tunnels
			.iter()
			.map(|(port, (host, host_port))| {
				(port.to_string(), json!({ "host": host, "port": host_port }))
			})
			.collect::<Map<_, _>>(),
	)
}

fn credentials_requested(params: &SandboxCreate) -> bool {
	params
		.credentials
		.as_ref()
		.is_some_and(|names| !names.is_empty())
}

fn network_required(params: &SandboxCreate) -> bool {
	!params.block_network
		|| credentials_requested(params)
		|| params.nics.as_ref().is_some_and(|nics| !nics.is_empty())
}

fn reject_macos_host_network_features(params: &SandboxCreate) -> Result<()> {
	if params.nics.as_ref().is_some_and(|nics| !nics.is_empty()) {
		return Err(EngineError::invalid("VPCs require a Linux host"));
	}
	for (feature, requested) in [
		("ports", params.ports.as_ref().is_some_and(|v| !v.is_empty())),
		("egress_allow", params.egress_allow.as_ref().is_some_and(|v| !v.is_empty())),
		(
			"egress_allow_domains",
			params
				.egress_allow_domains
				.as_ref()
				.is_some_and(|v| !v.is_empty()),
		),
		(
			"inbound_cidr_allowlist",
			params
				.inbound_cidr_allowlist
				.as_ref()
				.is_some_and(|v| !v.is_empty()),
		),
	] {
		if requested {
			return Err(EngineError::invalid(format!(
				"{feature} requires Linux host networking (TAP); macOS user-mode NAT is \
				 outbound-egress only"
			)));
		}
	}
	Ok(())
}

fn template_key_for_cached(cached: &CachedTemplate) -> String {
	template_key(
		&cached.spec.reference,
		cached.disk_mb,
		cached.memory,
		cached.cpus,
		cached.fs_slots,
		cached.host_slot,
		cached.nic_slot,
		cached.tap_slot,
	)
}

fn template_request_from_pool(reference: &str, extra: &HashMap<String, Value>) -> TemplateRequest {
	TemplateRequest {
		image:      Some(reference.to_owned()),
		dockerfile: extra
			.get("dockerfile")
			.and_then(Value::as_str)
			.map(PathBuf::from),
		context:    extra
			.get("context")
			.and_then(Value::as_str)
			.map_or_else(|| PathBuf::from("."), PathBuf::from),
		disk_mb:    extra.get("disk_mb").and_then(Value::as_u64).unwrap_or(1024),
		timeout:    extra.get("timeout").and_then(Value::as_u64).unwrap_or(300),
		memory:     extra.get("memory").and_then(Value::as_u64).unwrap_or(512),
		cpus:       extra.get("cpus").and_then(Value::as_u64).unwrap_or(1),
		fs_slots:   extra.get("fs_slots").and_then(Value::as_u64).unwrap_or(0),
		host_slot:  extra
			.get("host_slot")
			.and_then(Value::as_bool)
			.unwrap_or(false),
		// Default to the pool-eligible flavor (block_network templates have no
		// NIC slot); operators opt into the macOS networked-warm flavor with
		// `nic_slot: true`.
		nic_slot:   extra
			.get("nic_slot")
			.and_then(Value::as_bool)
			.unwrap_or(false),
		tap_slot:   extra
			.get("tap_slot")
			.and_then(Value::as_bool)
			.unwrap_or(false),
	}
}

fn control_for_vm(vm: &SandboxVm) -> Result<ControlClient> {
	ControlClient::connect(vm.control_sock()?, CONTROL_TIMEOUT)
}

fn snapshot_state_present(dir: &Path) -> bool {
	dir.join("current-generation").is_file()
}

fn teardown_network(name: &str) {
	let Some(lease) = net::lease_for(name) else {
		return;
	};
	let _ = net::teardown_tap(
		&lease.tap,
		Some(&lease.guest_ip),
		Some(&lease.host_ip),
		lease.prefix,
		None,
		None,
	);
	let _ = net::release_guest_config(name);
}

fn detect_oom(name: &str) -> bool {
	let Ok(meta) = SandboxVm::new(name).meta() else {
		return false;
	};
	let mut candidates = Vec::new();
	for key in ["memory_events", "memory_events_path"] {
		if let Some(path) = meta.get(key).and_then(Value::as_str) {
			candidates.push(PathBuf::from(path));
		}
	}
	if let Some(path) = meta
		.get("cgroup_path")
		.or_else(|| meta.get("cgroup"))
		.or_else(|| meta.get("cgroup_dir"))
		.and_then(Value::as_str)
	{
		candidates.push(PathBuf::from(path).join("memory.events"));
	}
	candidates.push(
		PathBuf::from("/sys/fs/cgroup")
			.join("vmon")
			.join(name)
			.join("memory.events"),
	);
	for path in candidates {
		let Ok(text) = fs::read_to_string(path) else {
			continue;
		};
		for line in text.lines() {
			let mut parts = line.split_whitespace();
			if parts.next() == Some("oom_kill")
				&& parts
					.next()
					.and_then(|value| value.parse::<u64>().ok())
					.unwrap_or(0)
					> 0
			{
				return true;
			}
		}
	}
	false
}

fn drain_entry_stream(
	rx: std::sync::mpsc::Receiver<Vec<u8>>,
	log: Arc<Mutex<fs::File>>,
) -> thread::JoinHandle<()> {
	thread::spawn(move || {
		for chunk in rx {
			let mut log = log.lock();
			if log.write_all(&chunk).and_then(|()| log.flush()).is_err() {
				break;
			}
		}
	})
}

fn bridge_bytes(rx: std::sync::mpsc::Receiver<Vec<u8>>) -> Receiver<Vec<u8>> {
	let (tx, out) = flume::unbounded();
	thread::spawn(move || {
		for chunk in rx {
			if tx.send(chunk).is_err() {
				break;
			}
		}
	});
	out
}

fn bridge_exit(
	rx: std::sync::mpsc::Receiver<Result<crate::engine::agent::ExitStatus>>,
) -> Receiver<ExecExit> {
	let (tx, out) = flume::bounded(1);
	thread::spawn(move || {
		let exit = match rx.recv() {
			Ok(Ok(status)) => ExecExit { code: status.code, signal: status.signal },
			Ok(Err(_)) | Err(_) => ExecExit { code: -1, signal: None },
		};
		let _ = tx.send(exit);
	});
	out
}

fn clamp_exec_timeout(timeout: Option<f64>) -> Duration {
	let Some(timeout) = timeout else {
		return EXEC_CAPTURE_CAP;
	};
	if !timeout.is_finite() || timeout <= 0.0 {
		return Duration::ZERO;
	}
	Duration::from_secs_f64(timeout).min(EXEC_CAPTURE_CAP)
}

/// Backstop for the VMM-armed deadline: every launch/claim path passes
/// `--timeout-secs` (or arms it via control `extend`), so the VMM self-kills
/// with return code 124 on time. This thread only cleans up a VM that
/// outlives its deadline by a grace period (a wedged VMM), so it never races
/// the self-kill's `status.json` write.
fn start_timeout_watchdog(name: String, secs: u64, runtime: Arc<dyn SandboxRuntime>) -> Sender<()> {
	const GRACE: Duration = Duration::from_secs(5);
	let (tx, rx) = flume::bounded(1);
	thread::Builder::new()
		.name(format!("vmon-timeout-{name}"))
		.spawn(move || {
			if rx
				.recv_timeout(Duration::from_secs(secs.max(1)) + GRACE)
				.is_err()
			{
				let vm = runtime.sandbox(&name);
				if runtime.is_running(&vm).unwrap_or(false) {
					let _ = runtime.stop(&vm, false);
				}
			}
		})
		.ok();
	tx
}

fn random_hex(bytes: usize) -> String {
	let mut out = String::with_capacity(bytes * 2);
	while out.len() < bytes * 2 {
		let _ = write!(out, "{:016x}", rand::random::<u64>());
	}
	out.truncate(bytes * 2);
	out
}

fn unix_time() -> f64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0.0, |duration| duration.as_secs_f64())
}

fn unix_millis() -> u128 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |duration| duration.as_millis())
}

fn idle_deadline_elapsed(
	record: &VmRecord,
	legacy_timeout: f64,
	legacy_sample_active: Option<bool>,
	now: f64,
) -> bool {
	if let Some(timeout) = record
		.detail
		.get("idle_timeout_secs")
		.and_then(Value::as_f64)
	{
		return timeout > 0.0 && now - record.last_network_active >= timeout;
	}
	legacy_sample_active == Some(false)
		&& legacy_timeout > 0.0
		&& now - record.last_active >= legacy_timeout
}

const fn network_delta_exceeds_threshold(delta: u64, threshold: u64) -> bool {
	delta > threshold
}

fn path_size(path: &Path) -> Result<u64> {
	let metadata = match fs::symlink_metadata(path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(0),
		Err(error) => return Err(error.into()),
	};
	if !metadata.is_dir() {
		return Ok(metadata.len());
	}
	let mut size = 0_u64;
	for entry in fs::read_dir(path)? {
		size = size.saturating_add(path_size(&entry?.path())?);
	}
	Ok(size)
}

fn remove_path(path: &Path) -> Result<()> {
	let metadata = match fs::symlink_metadata(path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
		Err(error) => return Err(error.into()),
	};
	if metadata.is_dir() {
		fs::remove_dir_all(path)?;
	} else {
		fs::remove_file(path)?;
	}
	Ok(())
}

const fn backend_name() -> &'static str {
	if cfg!(target_os = "macos") {
		"hvf"
	} else if cfg!(target_os = "linux") {
		"kvm"
	} else {
		"unknown"
	}
}

#[cfg(test)]
#[path = "rollback_acceptance.rs"]
mod rollback_acceptance;

#[cfg(test)]
mod tests {
	use tempfile::TempDir;

	use super::*;
	use crate::image::cas;

	fn config_for(temp: &TempDir) -> ServeConfig {
		let mut config = ServeConfig::default();
		config.home = temp.path().to_path_buf();
		config.warm_images = Vec::new();
		config
	}

	#[test]
	fn maintenance_interval_tracks_short_override_with_long_or_disabled_default() {
		for default_idle_timeout in [300.0, 0.0] {
			let temp = TempDir::new().expect("temp");
			let mut config = config_for(&temp);
			config.idle_timeout = default_idle_timeout;
			let (engine, _home) = Engine::new_test(config);
			let mut record = VmRecord::new("sandbox", "sandbox", "running");
			record.detail = json!({"idle_timeout_secs": 0.0});
			engine.insert_test_record(record);
			assert_eq!(engine.maintenance_interval(), Duration::from_secs(30));

			engine
				.inner
				.registry
				.update("sandbox", |record| {
					record.detail["idle_timeout_secs"] = json!(1.0);
				})
				.expect("idle policy update");
			assert_eq!(engine.maintenance_interval(), Duration::from_secs(1));
		}
	}
	fn insert_stopped_resize_fixture(
		engine: &Engine,
		temp: &TempDir,
		id: &str,
		with_rootfs: bool,
		requires_secrets: bool,
	) {
		let params = SandboxCreate {
			name: Some(id.to_owned()),
			cpus: 2,
			memory: 1024,
			disk_mb: 100,
			..Default::default()
		};
		let mut detail = json!({
			"cpus": 2,
			"memory": 1024,
			"disk_mb": 100,
			"cold_start_requires_process_secrets": requires_secrets,
		});
		if !requires_secrets {
			detail["relaunch_recipe"] = json!({
				"params": params,
				"template_dir": temp.path().join("templates/base"),
				"image_ref": Value::Null,
			});
		}
		write_meta(temp.path(), id, detail.clone());
		if with_rootfs {
			fs::write(engine.sandbox(id).dir().join("rootfs.img"), b"rootfs").expect("rootfs");
		}
		let mut record = VmRecord::new(id, id, "stopped");
		record.detail = detail;
		engine.insert_test_record(record);
	}

	#[test]
	fn cold_start_rejects_unknown_sandbox() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let error = engine.cold_start("missing").expect_err("unknown sandbox");
		assert_eq!(error.code, crate::error::ErrorCode::NotFound);
	}

	#[test]
	fn resume_stopped_without_retained_disk_is_not_found() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		insert_stopped_resize_fixture(&engine, &temp, "stopped", false, false);
		let error = engine.resume("stopped").expect_err("missing retained disk");
		assert_eq!(error.code, crate::error::ErrorCode::NotFound);
	}

	#[test]
	fn cold_start_after_secret_recipe_loss_is_busy() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		insert_stopped_resize_fixture(&engine, &temp, "secret", false, true);
		let error = engine.cold_start("secret").expect_err("lost secret recipe");
		assert_eq!(error.code, crate::error::ErrorCode::Busy);
		assert!(
			error
				.message
				.contains("sandbox used secrets and the server restarted")
		);
	}

	#[test]
	fn resize_validates_and_updates_stopped_shape_without_launching() {
		let temp = TempDir::new().expect("temp");
		let (engine, runtime, _home) = snapshot_engine(&temp, usize::MAX);
		insert_stopped_resize_fixture(&engine, &temp, "resize", true, false);
		assert_eq!(
			engine
				.resize("resize", None, None, None)
				.expect_err("empty resize")
				.code,
			crate::error::ErrorCode::Invalid
		);
		assert_eq!(
			engine
				.resize("resize", None, None, Some(99))
				.expect_err("disk shrink")
				.code,
			crate::error::ErrorCode::Invalid
		);
		let view = engine
			.resize("resize", Some(4), Some(2048), None)
			.expect("resize shape");
		assert_eq!(view["cpus"], 4);
		assert_eq!(view["memory"], 2048);
		assert_eq!(view["status"], "stopped");
		*engine.inner.disk_resize_executor.lock() = Some(Arc::new(|source, stage, bytes| {
			fs::copy(source, stage)?;
			OpenOptions::new().write(true).open(stage)?.set_len(bytes)?;
			Ok(())
		}));
		let view = engine
			.resize("resize", None, None, Some(101))
			.expect("grow disk");
		assert_eq!(view["disk_mb"], 101);
		assert_eq!(
			fs::metadata(engine.sandbox("resize").dir().join("rootfs.img"))
				.expect("resized disk")
				.len(),
			101 * 1024 * 1024
		);
		assert_eq!(runtime.launches.load(Ordering::Relaxed), 0);
	}

	fn checkpoint_fixture(temp: &TempDir, name: &str) -> PathBuf {
		let dir = temp.path().join(name);
		fs::create_dir(&dir).expect("checkpoint dir");
		fs::write(dir.join("rootfs.img"), name.as_bytes()).expect("rootfs");
		fs::write(dir.join("agent-ready.json"), b"{}").expect("agent marker");
		dir
	}

	fn indexed_checkpoint(temp: &TempDir, name: &str) -> (PathBuf, String) {
		let dir = checkpoint_fixture(temp, name);
		let digest = cas::index_template(&dir, None).expect("index checkpoint");
		assert_eq!(cas::lookup(&digest).expect("lookup indexed checkpoint"), Some(dir.clone()));
		(dir, digest)
	}

	fn write_meta(home: &Path, name: &str, meta: Value) {
		let dir = home.join("vms").join(name);
		fs::create_dir_all(&dir).expect("vm dir");
		fs::write(dir.join("meta.json"), serde_json::to_string_pretty(&meta).expect("json"))
			.expect("meta");
	}

	#[test]
	fn startup_reclaims_only_orphaned_disk_artifacts() {
		let temp = TempDir::new().expect("temp");
		let snapshots = temp.path().join("snapshots");
		let orphan = snapshots.join("orphan");
		let disk = orphan.join("disk");
		fs::create_dir_all(&disk).expect("disk artifact");
		fs::write(disk.join("rootfs.img"), b"disk payload").expect("rootfs");
		fs::write(
			disk.join("manifest.json"),
			serde_json::to_vec(&json!({
				"version": 1,
				"rootfs": "rootfs.img",
				"boot": "cold",
			}))
			.expect("manifest json"),
		)
		.expect("manifest");
		fs::create_dir(orphan.join(".disk.123.0.tmp")).expect("staging");

		let active = snapshots.join("active");
		fs::create_dir_all(&active).expect("active snapshot");
		fs::write(active.join("state"), b"referenced snapshot state").expect("state");

		let (engine, _home) = Engine::new_test(config_for(&temp));
		assert!(!engine.snapshot_dir("orphan").join("disk").exists());
		assert!(
			!engine
				.snapshot_dir("orphan")
				.join(".disk.123.0.tmp")
				.exists()
		);
		assert!(engine.snapshot_dir("active").join("state").is_file());
	}

	#[test]
	fn disk_capture_tree_has_one_rootfs_payload() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let artifact = temp.path().join("artifact");
		let capture = temp.path().join("capture");
		fs::create_dir(&artifact).expect("artifact");
		fs::create_dir(&capture).expect("capture");
		let source = artifact.join("rootfs.img");
		fs::write(&source, b"disk payload").expect("rootfs");
		move_or_copy_regular_file(&source, &capture.join("rootfs.img")).expect("move rootfs");
		fs::write(artifact.join("manifest.json"), b"{}").expect("manifest");
		fs::copy(artifact.join("manifest.json"), capture.join("disk-manifest.json"))
			.expect("copy manifest");

		let archive = temp.path().join("capture.venc");
		EncryptedArchive::seal(&capture, &archive, &engine.inner.keyring, "default").expect("seal");
		let opened =
			EncryptedArchive::open(&archive, &temp.path().join("opened"), &engine.inner.keyring)
				.expect("open");
		assert!(opened.join("rootfs.img").is_file());
		assert!(opened.join("disk-manifest.json").is_file());
		assert!(!opened.join("disk").exists());
		assert!(!artifact.join("rootfs.img").exists());
	}

	#[cfg(unix)]
	#[test]
	fn migration_stage_cow_copy_preserves_sparse_regular_files_and_rejects_symlinks() {
		use std::os::unix::fs::{FileExt, MetadataExt};

		let temp = TempDir::new().expect("temp");
		let source = temp.path().join("source");
		let destination = temp.path().join("destination");
		fs::create_dir(&source).expect("source");
		let sparse = fs::File::create(source.join("rootfs.img")).expect("sparse source");
		sparse
			.set_len(64 * 1024 * 1024)
			.expect("set logical length");
		sparse.write_at(b"first", 0).expect("write first extent");
		sparse
			.write_at(b"last", 64 * 1024 * 1024 - 4)
			.expect("write last extent");
		sparse.sync_all().expect("sync sparse source");

		copy_checkpoint_tree_cow(&source, &destination).expect("stage sparse checkpoint");
		let staged = fs::File::open(destination.join("rootfs.img")).expect("open staged rootfs");
		assert_eq!(staged.metadata().expect("staged metadata").len(), 64 * 1024 * 1024);
		assert!(
			staged.metadata().expect("staged allocation").blocks() < 2048,
			"staged sparse image must not be densely materialized"
		);
		let mut tail = [0; 4];
		staged
			.read_at(&mut tail, 64 * 1024 * 1024 - 4)
			.expect("read staged tail");
		assert_eq!(&tail, b"last");

		let linked = temp.path().join("linked");
		fs::create_dir(&linked).expect("linked source");
		std::os::unix::fs::symlink(temp.path(), linked.join("escape")).expect("symlink");
		assert!(copy_checkpoint_tree_cow(&linked, &temp.path().join("rejected")).is_err());
	}

	fn valid_create() -> SandboxCreate {
		SandboxCreate { cpus: 1, memory: 512, disk_mb: 1024, ..SandboxCreate::default() }
	}

	#[test]
	fn persistent_volume_data_is_sealed_between_mounts() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let volume = Volume::new_in_home(temp.path(), "workspace").expect("volume");
		fs::write(volume.path().join("legacy.txt"), b"legacy").expect("legacy data");

		let mut mounted = engine
			.materialize_encrypted_volume("sandbox", &volume, "default", false)
			.expect("materialize");
		assert_eq!(fs::read(mounted.mount_dir.join("legacy.txt")).expect("read"), b"legacy");
		assert!(!volume.path().join("legacy.txt").exists());
		fs::write(mounted.mount_dir.join("current.txt"), b"current").expect("guest write");
		mounted.arm();
		mounted.seal(&engine.inner.keyring).expect("seal");
		drop(mounted);

		let reopened = engine
			.materialize_encrypted_volume("sandbox", &volume, "default", true)
			.expect("reopen");
		assert_eq!(
			fs::read(reopened.mount_dir.join("current.txt")).expect("read current"),
			b"current"
		);
		assert!(encrypted_volume_archive(engine.home(), "workspace").is_file());
	}

	#[test]
	fn orphaned_volume_staging_is_resealed_on_startup_recovery() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let volume = Volume::new_in_home(temp.path(), "workspace").expect("volume");
		let mut mounted = engine
			.materialize_encrypted_volume("orphan", &volume, "default", false)
			.expect("materialize");
		fs::write(mounted.mount_dir.join("recovered.txt"), b"after crash").expect("guest write");
		mounted.arm();
		drop(mounted);

		engine
			.recover_orphaned_encrypted_volumes()
			.expect("recover staging");
		assert!(
			!encrypted_volume_runtime_root(engine.home())
				.join("orphan")
				.exists()
		);
		let reopened = engine
			.materialize_encrypted_volume("orphan", &volume, "default", true)
			.expect("reopen");
		assert_eq!(
			fs::read(reopened.mount_dir.join("recovered.txt")).expect("read recovered"),
			b"after crash"
		);
	}

	#[test]
	fn archived_volume_reopen_removes_leftover_plaintext() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let volume = Volume::new_in_home(temp.path(), "workspace").expect("volume");
		let mut mounted = engine
			.materialize_encrypted_volume("first", &volume, "default", false)
			.expect("materialize");
		fs::write(mounted.mount_dir.join("current.txt"), b"current").expect("guest write");
		mounted.arm();
		mounted.seal(&engine.inner.keyring).expect("seal");
		drop(mounted);
		fs::write(volume.path().join("leftover.txt"), b"plaintext").expect("leftover");

		let reopened = engine
			.materialize_encrypted_volume("second", &volume, "default", true)
			.expect("reopen");
		assert_eq!(fs::read(reopened.mount_dir.join("current.txt")).expect("read"), b"current");
		assert!(!volume.path().join("leftover.txt").exists());
	}

	#[test]
	fn recovery_pruning_never_deletes_the_protected_point() {
		let temp = TempDir::new().expect("temp");
		let mut config = config_for(&temp);
		config.history_retention = 1;
		let (engine, _home) = Engine::new_test(config);
		let root = engine.recovery_root("sandbox").expect("root");
		fs::create_dir_all(&root).expect("mkdir");
		fs::write(root.join("00000000000000000001-disk-old.venc"), b"old").expect("old");
		fs::write(root.join("00000000000000000000-disk-new.venc"), b"new").expect("new");

		engine
			.prune_recovery("sandbox", Some("00000000000000000000-disk-new"))
			.expect("prune");
		assert!(root.join("00000000000000000000-disk-new.venc").is_file());
	}

	#[test]
	fn removing_a_sandbox_deletes_its_local_state() {
		let temp = TempDir::new().expect("temp");
		let (engine, _runtime, _home) = snapshot_engine(&temp, usize::MAX);
		let record = VmRecord::new("stable-id", "vm-directory", "running");
		engine
			.inner
			.registry
			.insert_persisted(engine.home(), record)
			.expect("persist record");
		let runtime_dir = engine.sandbox("vm-directory").dir().to_path_buf();
		let recovery = engine.recovery_root("stable-id").expect("recovery root");
		fs::create_dir_all(&recovery).expect("recovery directory");
		fs::write(recovery.join("point.venc"), b"point").expect("recovery point");

		engine.remove("stable-id").expect("remove sandbox");

		assert!(!recovery.exists(), "sandbox removal retained local recovery history");
		assert!(!runtime_dir.exists(), "sandbox removal retained its runtime directory");
		let restarted = Registry::new();
		restarted.rehydrate(engine.home()).expect("rehydrate");
		assert!(restarted.list().is_empty(), "removed sandbox was rehydrated");
	}

	#[test]
	fn rehydrated_runtime_cannot_checkpoint_without_identity() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let mut record = VmRecord::new("stable-id", "vm-directory", "running");
		record.detail = json!({"block_network": true});
		engine.insert_test_record(record);
		engine
			.inner
			.runtimes
			.lock()
			.insert("vm-directory".to_owned(), RuntimeState::default());

		let error = engine
			.mesh_checkpoint_params("stable-id")
			.expect_err("rehydrated runtime rejected");
		assert_eq!(error.code.as_str(), "unsupported");
	}

	#[test]
	fn snapshot_metadata_must_be_a_bounded_regular_file() {
		let temp = TempDir::new().expect("temp");
		let regular = temp.path().join("regular.json");
		fs::write(&regular, b"{}").expect("regular metadata");
		assert_eq!(read_snapshot_metadata(&regular).expect("read metadata"), b"{}");

		let outside = temp.path().join("outside.json");
		fs::write(&outside, b"{}").expect("outside metadata");
		let symlink = temp.path().join("symlink.json");
		std::os::unix::fs::symlink(&outside, &symlink).expect("metadata symlink");
		assert!(read_snapshot_metadata(&symlink).is_err());

		let oversized = temp.path().join("oversized.json");
		fs::write(&oversized, vec![b' '; MAX_SNAPSHOT_METADATA_BYTES as usize + 1])
			.expect("oversized metadata");
		assert!(read_snapshot_metadata(&oversized).is_err());
	}

	#[test]
	fn snapshot_options_accept_blocked_network_metadata() {
		let options = resolve_snapshot_options(
			HashMap::from([("block_network".to_owned(), Value::Bool(true))]),
			None,
		)
		.expect("blocked-network snapshot options");

		assert_eq!(options.block_network, Some(true));
	}

	#[test]
	fn mesh_checkpoint_carries_secret_environment() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let mut record = VmRecord::new("secret", "secret", "running");
		record.detail = json!({"block_network": true});
		engine.insert_test_record(record);

		let mut runtime = RuntimeState { identity_complete: true, ..RuntimeState::default() };
		runtime
			.secret_env
			.insert("TOKEN".to_owned(), "sensitive".to_owned());
		engine
			.inner
			.runtimes
			.lock()
			.insert("secret".to_owned(), runtime);

		let (_, params) = engine
			.mesh_checkpoint_params("secret")
			.expect("secret-bearing sandbox can migrate");
		assert_eq!(params["secrets"][0]["name"], "carried");
		assert_eq!(params["secrets"][0]["values"]["TOKEN"], "sensitive");
	}

	#[test]
	fn lifecycle_events_are_sequenced_and_describe_resulting_state() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let events = engine.subscribe_events();
		let mut record = VmRecord::new("sandbox", "sandbox", "running");
		record.source = Some("alpine".to_owned());
		record.tags.insert("role".to_owned(), "worker".to_owned());

		engine.publish_record_event("ready", &record);
		record.status = "terminated".to_owned();
		record.detail = json!({"returncode": 137, "terminated_reason": "oom"});
		engine.publish_record_event("terminated", &record);
		engine.publish_record_event("removed", &record);

		let ready = events.recv().expect("ready event");
		assert_eq!(ready["type"], "ready");
		assert_eq!(ready["sequence"], 1);
		assert_eq!(ready["status"], "running");
		assert_eq!(ready["source"], "alpine");
		assert_eq!(ready["tags"]["role"], "worker");
		let terminated = events.recv().expect("terminated event");
		assert_eq!(terminated["type"], "terminated");
		assert_eq!(terminated["sequence"], 2);
		assert_eq!(terminated["status"], "terminated");
		assert_eq!(terminated["returncode"], 137);
		assert_eq!(terminated["reason"], "oom");
		let removed = events.recv().expect("removed event");
		assert_eq!(removed["type"], "removed");
		assert_eq!(removed["sequence"], 3);
		assert_eq!(removed["status"], "removed");
	}

	struct SnapshotRuntime {
		launches: std::sync::atomic::AtomicUsize,
		fail_at:  usize,
		names:    Mutex<Vec<String>>,
	}

	impl SnapshotRuntime {
		fn new(fail_at: usize) -> Arc<Self> {
			Arc::new(Self {
				launches: std::sync::atomic::AtomicUsize::new(0),
				fail_at,
				names: Mutex::new(Vec::new()),
			})
		}

		fn names(&self) -> Vec<String> {
			self.names.lock().clone()
		}
	}

	impl SandboxRuntime for SnapshotRuntime {
		fn name(&self) -> &'static str {
			"snapshot-test"
		}

		fn launch(&self, vm: &SandboxVm, _spec: &LaunchSpec) -> Result<()> {
			let attempt = self.launches.fetch_add(1, Ordering::Relaxed) + 1;
			self.names.lock().push(vm.name().to_owned());
			fs::create_dir_all(vm.dir())?;
			if attempt == self.fail_at {
				return Err(EngineError::engine("injected snapshot launch failure"));
			}
			vm.save_meta(Map::from_iter([("pid".to_owned(), json!(1000 + attempt))]))
		}

		fn stop(&self, _vm: &SandboxVm, _wait: bool) -> Result<()> {
			Ok(())
		}

		fn remove(&self, vm: &SandboxVm) -> Result<()> {
			match fs::remove_dir_all(vm.dir()) {
				Ok(()) => Ok(()),
				Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
				Err(error) => Err(error.into()),
			}
		}

		fn is_running(&self, vm: &SandboxVm) -> Result<bool> {
			Ok(vm.dir().is_dir())
		}
	}

	fn snapshot_engine(
		temp: &TempDir,
		fail_at: usize,
	) -> (Engine, Arc<SnapshotRuntime>, crate::home::test_home::HomeGuard) {
		let home = crate::home::test_home::set(temp.path());
		let runtime = SnapshotRuntime::new(fail_at);
		let engine =
			Engine::with_runtime(config_for(temp), runtime.clone()).expect("snapshot engine");
		(engine, runtime, home)
	}

	#[test]
	fn create_resources_survive_worker_rehydrate() {
		let temp = TempDir::new().expect("temp");
		let (engine, _runtime, _home) = snapshot_engine(&temp, usize::MAX);
		let template = checkpoint_fixture(&temp, "template");
		fs::write(template.join("current-generation"), b"1\n").expect("generation");
		let params = Map::from_iter([
			("name".to_owned(), json!("sandbox")),
			("template".to_owned(), json!(template)),
			("cpus".to_owned(), json!(2)),
			("memory".to_owned(), json!(768)),
			("disk_mb".to_owned(), json!(1024)),
		]);

		engine
			.mesh_create_from_params_paused(params)
			.expect("create paused candidate");
		let restarted = Registry::new();
		restarted.rehydrate(engine.home()).expect("rehydrate");

		let restored = restarted.get("sandbox").expect("sandbox record");
		assert_eq!(restored.detail["cpus"], 2);
		assert_eq!(restored.detail["memory"], 768);
		assert_eq!(restored.detail["disk_mb"], 1024);
	}
	fn seal_test_snapshot(engine: &Engine, name: &str) {
		let source = engine.snapshot_dir(&format!(".{name}-source"));
		fs::create_dir_all(&source).expect("snapshot source");
		fs::write(source.join("snapshot.json"), b"{}").expect("snapshot metadata");
		EncryptedArchive::seal(
			&source,
			&engine
				.snapshot_archive(name)
				.expect("snapshot archive path"),
			&engine.inner.keyring,
			"default",
		)
		.expect("seal test snapshot");
		fs::remove_dir_all(source).expect("remove snapshot source");
	}

	#[test]
	fn named_snapshot_source_is_decrypted_once_until_delete() {
		let temp = TempDir::new().expect("temp");
		let (engine, _runtime, _home) = snapshot_engine(&temp, usize::MAX);
		seal_test_snapshot(&engine, "base");

		let first = engine.open_snapshot("base").expect("first open");
		let second = engine.open_snapshot("base").expect("cached open");
		assert_eq!(first.path, second.path);
		assert!(Arc::ptr_eq(
			first.guard.as_ref().expect("first guard"),
			second.guard.as_ref().expect("second guard")
		));
		let extracted = first.path.clone();
		drop(first);
		drop(second);
		assert!(extracted.exists(), "cache must retain the decrypted template");

		engine.snapshot_delete("base").expect("delete snapshot");
		assert!(!extracted.exists(), "deleting a snapshot must evict its decrypted template");
	}

	#[test]
	fn replacement_lifecycle_preserves_nonzero_resume_and_rollback_generations() {
		let resume = LifecycleState {
			desired:    LifecyclePhase::Running,
			observed:   LifecyclePhase::Suspended,
			generation: StateGeneration(3),
			failure:    None,
			operation:  None,
		};
		let restored_resume = Engine::replacement_lifecycle(&resume);
		assert_eq!(restored_resume.generation, StateGeneration(3));
		assert_eq!(restored_resume.operation, None);

		let rollback = LifecycleState {
			desired:    LifecyclePhase::Running,
			observed:   LifecyclePhase::Running,
			generation: StateGeneration(7),
			failure:    None,
			operation:  Some(LifecycleOperation::Rollback { recovery_point: "point-7".to_owned() }),
		};
		let mut restored_rollback = Engine::replacement_lifecycle(&rollback);
		assert_eq!(restored_rollback.generation, StateGeneration(7));
		assert_eq!(restored_rollback.operation, rollback.operation);
		restored_rollback.operation = None;
		assert!(restored_rollback.is_converged());
	}
	#[test]
	fn replacement_never_rewinds_checkpoint_generation() {
		let previous = json!({ "checkpoint_generation": 19 });
		let restored = Map::from_iter([("checkpoint_generation".to_owned(), json!(4))]);
		assert_eq!(Engine::replacement_checkpoint_generation(&previous, &restored), 19);
	}
	#[test]
	fn rollback_journal_is_durable_and_contains_preteardown_counter() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let journal = RollbackJournal {
			sandbox_id:            "rollback".to_owned(),
			target_recovery_point: "target".to_owned(),
			safety_recovery_point: "safety".to_owned(),
			generation:            7,
			checkpoint_generation: 19,
			portable_owner:        "node-a".to_owned(),
			portable_owner_epoch:  3,
			source_token:          "e634f1ac918b7513ebec28b04f425a9d".to_owned(),
		};
		engine
			.write_rollback_journal(&journal)
			.expect("write journal");
		let bytes = fs::read(
			engine
				.rollback_journal_path("rollback")
				.expect("journal path"),
		)
		.expect("read journal");
		assert_eq!(
			serde_json::from_slice::<RollbackJournal>(&bytes)
				.expect("decode durable journal")
				.checkpoint_generation,
			19
		);
		engine
			.clear_rollback_journal("rollback")
			.expect("clear journal");
		assert!(
			!engine
				.rollback_journal_path("rollback")
				.expect("journal path")
				.exists()
		);
	}

	#[test]
	fn rollback_replay_retains_only_exact_committed_target_or_live_source() {
		assert_eq!(
			rollback_replay_decision(true, true, Some(&RollbackDisposition::CommittedRunning)),
			RollbackReplayDecision::AbortRetainSource,
		);
		assert_eq!(
			rollback_replay_decision(false, true, Some(&RollbackDisposition::CommittedRunning)),
			RollbackReplayDecision::FinalizeRetainTarget,
		);
		assert_eq!(
			rollback_replay_decision(false, true, Some(&RollbackDisposition::Other)),
			RollbackReplayDecision::RecoverSafety,
		);
		assert_eq!(
			rollback_replay_decision(false, false, Some(&RollbackDisposition::CommittedRunning)),
			RollbackReplayDecision::RecoverSafety,
		);
	}

	#[test]
	fn stale_placeholder_journal_does_not_block_a_newer_orphan_marker() {
		let journal = RollbackJournal {
			sandbox_id:            "sandbox".to_owned(),
			target_recovery_point: "target".to_owned(),
			safety_recovery_point: "safety".to_owned(),
			generation:            4,
			checkpoint_generation: 9,
			portable_owner:        "former-owner".to_owned(),
			portable_owner_epoch:  2,
			source_token:          "e634f1ac918b7513ebec28b04f425a9d".to_owned(),
		};
		assert!(!rollback_journal_matches_marker(&journal, "sandbox", "new-owner", 3, 5,));
		assert!(rollback_journal_matches_marker(&journal, "sandbox", "former-owner", 2, 4,));
	}

	#[test]
	fn internal_safety_recovery_is_hidden_and_cleaned_up() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let safety = "00000000000000000001-checkpoint-00000000000000000001-token-rollback-safety-";
		let normal = "00000000000000000002-checkpoint-00000000000000000002-token";
		for point in [safety, normal] {
			let archive = engine
				.recovery_archive("sandbox", point)
				.expect("archive path");
			fs::create_dir_all(archive.parent().expect("archive parent")).expect("recovery root");
			fs::write(archive, b"internal").expect("archive");
		}
		let points = engine.recovery_points("sandbox").expect("list history");
		assert_eq!(
			points
				.iter()
				.map(|point| point.name.as_str())
				.collect::<Vec<_>>(),
			[normal]
		);
		engine
			.remove_safety_recovery("sandbox", safety)
			.expect("cleanup safety");
		assert!(
			!engine
				.recovery_archive("sandbox", safety)
				.expect("archive path")
				.exists()
		);
	}

	#[test]
	fn restore_requires_a_snapshot_and_returns_a_canonical_view() {
		let temp = TempDir::new().expect("temp");
		let (engine, runtime, _home) = snapshot_engine(&temp, usize::MAX);

		let missing = engine
			.restore("missing", RestoreBody::default())
			.expect_err("missing snapshots must fail before launch");
		assert_eq!(missing.code.as_str(), "not_found");
		assert!(runtime.names().is_empty());

		for unsafe_name in ["../escape", "nested/snapshot", ".hidden", r"nested\snapshot"] {
			let error = engine
				.restore(unsafe_name, RestoreBody::default())
				.expect_err("unsafe snapshot name");
			assert_eq!(error.code.as_str(), "invalid");
			let error = engine
				.create(SandboxCreate { name: Some(unsafe_name.to_owned()), ..valid_create() })
				.expect_err("unsafe sandbox name");
			assert_eq!(error.code.as_str(), "invalid");
		}
		assert!(!temp.path().join("escape").exists());

		seal_test_snapshot(&engine, "base");
		std::os::unix::fs::symlink(
			temp.path(),
			engine
				.snapshot_archive("linked")
				.expect("linked archive path"),
		)
		.expect("snapshot symlink");
		let linked = engine
			.restore("linked", RestoreBody::default())
			.expect_err("snapshot symlinks must be rejected");
		assert_eq!(linked.code.as_str(), "invalid");
		let unsafe_target = engine
			.restore("base", RestoreBody {
				name: Some("../escape".to_owned()),
				..RestoreBody::default()
			})
			.expect_err("unsafe restore target");
		assert_eq!(unsafe_target.code.as_str(), "invalid");
		fs::create_dir_all(engine.home().templates_dir().join("image-template")).expect("template");
		assert_eq!(engine.snapshots().expect("snapshot list"), ["base"]);
		let view = engine
			.restore("base", RestoreBody {
				name: Some("restored".to_owned()),
				..RestoreBody::default()
			})
			.expect("restore");
		assert_eq!(view["id"], "restored");
		assert_eq!(view["name"], "restored");
		assert_eq!(view["status"], "running");
		assert_eq!(view["source"], "restore:base");
		assert!(
			view["created_at"]
				.as_f64()
				.is_some_and(|created| created > 0.0)
		);
		assert_eq!(engine.get("restored").expect("persisted view")["id"], "restored");

		let collision = engine
			.restore("base", RestoreBody {
				name: Some("restored".to_owned()),
				..RestoreBody::default()
			})
			.expect_err("restore must not overwrite a sandbox");
		assert_eq!(collision.code.as_str(), "busy");
		assert_eq!(runtime.names(), ["restored"]);
	}

	#[test]
	fn snapshot_launch_wakes_maintenance_for_short_idle_override() {
		let temp = TempDir::new().expect("temp");
		let (engine, _runtime, _home) = snapshot_engine(&temp, usize::MAX);
		seal_test_snapshot(&engine, "base");
		let wake = engine.inner.maintenance_wake.notified();

		engine
			.restore("base", RestoreBody {
				name:  Some("restored-idle".to_owned()),
				agent: None,
				extra: HashMap::from([("idle_timeout_secs".to_owned(), json!(1.0))]),
			})
			.expect("restore with short idle policy");
		engine.inner.net_runtime.handle().block_on(async {
			tokio::time::timeout(Duration::from_secs(1), wake)
				.await
				.expect("snapshot launch must wake maintenance");
		});
		assert_eq!(engine.maintenance_interval(), Duration::from_secs(1));
	}

	#[test]
	fn fork_rejects_invalid_counts_and_rolls_back_a_partial_batch() {
		let temp = TempDir::new().expect("temp");
		let (engine, runtime, _home) = snapshot_engine(&temp, 2);
		seal_test_snapshot(&engine, "base");

		for count in [0, MAX_FORK_CLONES + 1] {
			let error = engine
				.fork("base", ForkBody { count, extra: HashMap::new() })
				.expect_err("invalid count");
			assert_eq!(error.code.as_str(), "invalid");
		}
		assert!(runtime.names().is_empty());

		let error = engine
			.fork("base", ForkBody { count: 3, extra: HashMap::new() })
			.expect_err("second clone fails");
		assert_eq!(error.message, "injected snapshot launch failure");
		let names = runtime.names();
		assert_eq!(names.len(), 2);
		assert!(engine.list(None).expect("registry list").is_empty());
		for name in names {
			assert!(!temp.path().join("vms").join(name).exists());
		}
	}

	#[test]
	fn fork_returns_full_canonical_views() {
		let temp = TempDir::new().expect("temp");
		let (engine, _runtime, _home) = snapshot_engine(&temp, usize::MAX);
		seal_test_snapshot(&engine, "base");

		let value = engine
			.fork("base", ForkBody { count: 2, extra: HashMap::new() })
			.expect("fork");
		let clones = value["clones"].as_array().expect("clone views");
		assert_eq!(clones.len(), 2);
		for clone in clones {
			assert!(
				clone["id"]
					.as_str()
					.is_some_and(|id| id.starts_with("fork-"))
			);
			assert_eq!(clone["name"], clone["id"]);
			assert_eq!(clone["status"], "running");
			assert_eq!(clone["source"], "fork:base");
			assert!(clone["created_at"].as_f64().is_some());
		}
		assert_eq!(engine.list(None).expect("registry list").len(), 2);
	}

	#[test]
	fn idempotency_replays_rehydrated_record_without_launching() {
		let temp = TempDir::new().expect("temp");
		write_meta(
			temp.path(),
			"sb-existing",
			json!({ "status": "stopped", "idempotency_key": "idem", "tags": {"k": "v"}, "timeout_secs": 30 }),
		);
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let view = engine
			.create(SandboxCreate {
				idempotency_key: Some("idem".to_owned()),
				cpus: 0,
				..SandboxCreate::default()
			})
			.expect("replayed before launch");
		assert_eq!(view["name"], "sb-existing");
	}

	#[test]
	fn connect_token_is_stable_per_sandbox() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		engine.insert_test_record(VmRecord::new("sb", "sb", "running"));
		let first = engine.tunnels("sb").expect("first")["connect_token"]
			.as_str()
			.expect("token")
			.to_owned();
		let second = engine.tunnels("sb").expect("second")["connect_token"]
			.as_str()
			.expect("token")
			.to_owned();
		assert_eq!(first, second);
		assert!(!first.is_empty());
	}

	#[test]
	fn prometheus_text_contains_counters_and_statuses() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		engine.insert_test_record(VmRecord::new("sb", "sb", "running"));
		engine.inc_counter("created");
		let text = engine.prometheus_metrics();
		assert!(text.contains("# HELP vmon_server_sandboxes"));
		assert!(text.contains("vmon_server_sandboxes{status=\"running\"} 1"));
		assert!(text.contains("vmon_server_events_total{event=\"created\"} 1"));
		assert!(text.ends_with('\n'));
	}

	#[test]
	fn pool_stats_view_uses_template_key_format() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let key = template_key("ref", 1, 2, 3, 4, false, true, false);
		let pool =
			WarmPool::with_options("/no/template", 0, false, false, Duration::ZERO).expect("pool");
		engine.inner.pools.set(key.clone(), pool);
		let view = engine.pool_list().expect("pool list");
		assert_eq!(view[&key]["ready"], 0);
		assert_eq!(view[&key]["size"], 0);
		engine.shutdown();
	}

	#[test]
	fn template_memo_short_circuits_image_resolution() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let tpl_dir = temp.path().join("templates").join("tpl-memo");
		fs::create_dir_all(&tpl_dir).expect("template dir");
		fs::write(tpl_dir.join("current-generation"), b"1").expect("generation marker");
		fs::write(tpl_dir.join("rootfs.img"), b"base").expect("base disk");
		let cached = CachedTemplate {
			name:         "tpl-memo".to_owned(),
			snapshot_dir: tpl_dir.clone(),
			rootfs:       tpl_dir.join("rootfs.img"),
			spec:         image::ImageConfig {
				reference:  "memo/unresolvable:1".to_owned(),
				entrypoint: Vec::new(),
				cmd:        Vec::new(),
				env:        Vec::new(),
				workdir:    "/".to_owned(),
				user:       String::new(),
			},
			image_digest: "sha256:0".to_owned(),
			disk_mb:      1024,
			memory:       512,
			cpus:         1,
			fs_slots:     0,
			host_slot:    false,
			nic_slot:     false,
			tap_slot:     false,
			digest:       String::new(),
		};
		let key = TemplateMemoKey {
			image:     Some("memo/unresolvable:1".to_owned()),
			disk_mb:   1024,
			memory:    512,
			cpus:      1,
			fs_slots:  0,
			host_slot: false,
			nic_slot:  false,
			tap_slot:  false,
		};
		engine
			.inner
			.template_memo
			.lock()
			.insert(key.clone(), cached);
		// The reference is deliberately unresolvable: falling through to the
		// OCI/template pipeline would fail this resolve. A memo hit must not
		// touch that pipeline at all.
		let params =
			SandboxCreate { image: Some("memo/unresolvable:1".to_owned()), ..valid_create() };
		let (dir, spec, name, pool_key) = engine
			.resolve_template(&params, 0, false, false, false)
			.expect("memoized template resolves without the image pipeline");
		assert_eq!(dir, tpl_dir);
		assert_eq!(spec.map(|spec| spec.reference).as_deref(), Some("memo/unresolvable:1"));
		assert_eq!(name.as_deref(), Some("tpl-memo"));
		assert_eq!(
			pool_key,
			template_key("memo/unresolvable:1", 1024, 512, 1, 0, false, false, false)
		);
		// An invalidated on-disk snapshot drops out of the memo.
		fs::remove_file(tpl_dir.join("current-generation")).expect("invalidate template");
		assert!(engine.lookup_template_memo(&key).is_none());
		assert!(engine.inner.template_memo.lock().is_empty());
		engine.shutdown();
	}

	#[test]
	fn exec_capture_timeout_is_capped_at_sixty_seconds() {
		assert_eq!(clamp_exec_timeout(None), Duration::from_mins(1));
		assert_eq!(clamp_exec_timeout(Some(120.0)), Duration::from_mins(1));
		assert_eq!(clamp_exec_timeout(Some(2.5)), Duration::from_millis(2500));
	}

	#[test]
	fn create_validation_matches_python_messages() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let err = engine
			.create(SandboxCreate { ha: Some("bad".to_owned()), ..valid_create() })
			.expect_err("invalid ha");
		assert_eq!(err.message, "ha must be one of: async, async+rerun, off, rerun");
		let err = engine
			.create(SandboxCreate { block_network: true, ports: Some(vec![80]), ..valid_create() })
			.expect_err("bad ports");
		assert_eq!(err.message, "ports cannot be exposed when block_network=True");
		let err = engine
			.create(SandboxCreate {
				remote_page_url: Some("http://peer".to_owned()),
				..valid_create()
			})
			.expect_err("remote page rejected");
		assert_eq!(err.message, "remote_page_* fields are server-internal");
	}

	#[test]
	fn named_create_publishes_failure_when_preparation_is_rejected() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let events = engine.subscribe_events();

		let error = engine
			.create(SandboxCreate {
				name: Some("rejected-create".to_owned()),
				ha: Some("bad".to_owned()),
				..valid_create()
			})
			.expect_err("invalid named create");

		let event = events
			.recv_timeout(Duration::from_secs(1))
			.expect("failed lifecycle event");
		assert_eq!(event["type"], "failed");
		assert_eq!(event["id"], "rejected-create");
		assert_eq!(event["name"], "rejected-create");
		assert_eq!(event["status"], "failed");
		assert_eq!(event["code"], error.code.as_str());
		assert_eq!(event["error"], error.message);
	}

	#[test]
	fn mesh_replicate_cleanup_drops_cas_pointer_after_export_handles_release() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let (snapshot_dir, digest) = indexed_checkpoint(&temp, "replica-checkpoint");
		engine.inner.pending_replica_exports.lock().insert(
			"replica".to_owned(),
			Arc::new(ReplicaExport {
				digest:       digest.clone(),
				snapshot_dir: snapshot_dir.clone(),
				_cleanup:     CheckpointCleanup::new(digest.clone(), snapshot_dir.clone()),
				object_key:   Mutex::new(None),
			}),
		);
		engine
			.mesh_bind_replica_export("replica", &digest, "replicas/replica/object")
			.expect("bind active export");
		engine
			.mesh_bind_replica_export("replica", &digest, "replicas/replica/object")
			.expect("same object binding is idempotent");
		assert!(
			engine
				.mesh_bind_replica_export("replica", &digest, "replicas/replica/other")
				.is_err(),
			"an export cannot be rebound to another object"
		);
		let stream_export = engine
			.mesh_replica_export("replica", &digest, "replicas/replica/object")
			.expect("active export");

		engine
			.mesh_replicate_cleanup(&digest, &snapshot_dir)
			.expect("replica cleanup succeeds");

		assert_eq!(cas::lookup(&digest).expect("lookup after cleanup"), None);
		assert!(snapshot_dir.exists(), "active stream handle must pin export directory");
		drop(stream_export);
		assert!(!snapshot_dir.exists(), "last export handle deletes the checkpoint");
		assert!(
			engine
				.mesh_replica_export("replica", &digest, "replicas/replica/object")
				.is_err(),
			"cleanup makes the export unavailable to new streams"
		);
	}
	#[test]
	fn abandoned_indexed_checkpoint_removes_cas_pointer_and_directory() {
		let temp = TempDir::new().expect("temp");
		let (snapshot_dir, digest) = indexed_checkpoint(&temp, "abandoned-checkpoint");
		drop(CheckpointCleanup::new(digest.clone(), snapshot_dir.clone()));
		assert_eq!(cas::lookup(&digest).expect("CAS lookup"), None);
		assert!(!snapshot_dir.exists(), "abandoned checkpoint directory is removed");
	}
	#[test]
	fn checkpoint_cleanup_does_not_unpublish_newer_same_digest_pointer() {
		let temp = TempDir::new().expect("temp");
		let (_engine, _home) = Engine::new_test(config_for(&temp));
		let first = checkpoint_fixture(&temp, "same-digest-a");
		let second = checkpoint_fixture(&temp, "same-digest-b");
		for dir in [&first, &second] {
			fs::write(dir.join("rootfs.img"), b"identical rootfs").expect("rootfs");
		}
		let digest = cas::index_template(&first, None).expect("index first");
		assert_eq!(cas::index_template(&second, Some(&digest)).expect("index second"), digest);
		assert_eq!(cas::lookup(&digest).expect("lookup"), Some(second.clone()));

		drop(CheckpointCleanup::new(digest.clone(), first.clone()));
		assert_eq!(
			cas::lookup(&digest).expect("first exact cleanup"),
			Some(second.clone()),
			"cleanup of the superseded checkpoint must preserve the newer pointer"
		);
		assert!(!first.exists());

		drop(CheckpointCleanup::new(digest.clone(), second.clone()));
		assert_eq!(cas::lookup(&digest).expect("second exact cleanup"), None);
		assert!(!second.exists());
	}
	#[test]
	fn portable_delete_commits_history_before_tombstone() {
		let events = Arc::new(Mutex::new(Vec::new()));
		Engine::commit_portable_delete_after_history(
			{
				let events = Arc::clone(&events);
				move || {
					events.lock().push("history");
					Ok(())
				}
			},
			{
				let events = Arc::clone(&events);
				move || {
					events.lock().push("commit");
					Ok(())
				}
			},
		)
		.expect("delete transaction");
		assert_eq!(*events.lock(), ["history", "commit"]);
	}

	#[test]
	fn portable_delete_history_failure_retains_tombstone_for_retry() {
		let committed = Arc::new(std::sync::atomic::AtomicBool::new(false));
		let error = Engine::commit_portable_delete_after_history(
			|| Err(EngineError::engine("history delete failed")),
			{
				let committed = Arc::clone(&committed);
				move || {
					committed.store(true, Ordering::Relaxed);
					Ok(())
				}
			},
		)
		.expect_err("history failure must prevent tombstone commit");
		assert_eq!(error.message, "history delete failed");
		assert!(
			!committed.load(Ordering::Relaxed),
			"uncommitted tombstone remains available for retry"
		);
	}

	#[test]
	fn mesh_migrate_commit_unknown_sid_still_drops_both_checkpoints() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let (base_dir, base_digest) = indexed_checkpoint(&temp, "commit-base-checkpoint");
		let (delta_dir, delta_digest) = indexed_checkpoint(&temp, "commit-delta-checkpoint");

		engine
			.mesh_migrate_commit("missing-source", &base_dir, &base_digest, &delta_dir, &delta_digest)
			.expect("unknown source must not block checkpoint cleanup");

		assert_eq!(cas::lookup(&base_digest).expect("base lookup after commit"), None);
		assert_eq!(cas::lookup(&delta_digest).expect("delta lookup after commit"), None);
		assert!(!base_dir.exists(), "migration commit must delete the pre-copy checkpoint");
		assert!(!delta_dir.exists(), "migration commit must delete the delta checkpoint");
	}

	#[test]
	fn mesh_migrate_abort_drops_pointers_but_keeps_checkpoint_dirs_after_create_success() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let (base_dir, base_digest) = indexed_checkpoint(&temp, "abort-base-checkpoint");
		let (delta_dir, delta_digest) = indexed_checkpoint(&temp, "abort-success-checkpoint");
		let mut record = VmRecord::new("restored", "restored", "running");
		record.detail = json!({ "idempotency_key": "abort-replay" });
		engine.insert_test_record(record);
		engine
			.inner
			.registry
			.record_idempotency("restored", "abort-replay");
		let mut params = Map::new();
		params.insert("idempotency_key".to_owned(), json!("abort-replay"));

		let view = engine
			.mesh_migrate_abort("missing-source", &base_digest, &delta_dir, &delta_digest, params)
			.expect("abort cleanup succeeds after create returns");

		assert_eq!(view["name"], "restored");
		assert_eq!(cas::lookup(&base_digest).expect("base lookup after abort"), None);
		assert_eq!(cas::lookup(&delta_digest).expect("delta lookup after abort"), None);
		assert!(base_dir.is_dir(), "migration abort must keep the pre-copy checkpoint");
		assert!(delta_dir.is_dir(), "migration abort must keep the delta checkpoint");
	}

	#[test]
	fn mesh_migrate_abort_create_error_propagates_and_keeps_checkpoint_dirs() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let (base_dir, base_digest) = indexed_checkpoint(&temp, "abort-error-base-checkpoint");
		let (delta_dir, delta_digest) = indexed_checkpoint(&temp, "abort-error-checkpoint");
		let mut params = Map::new();
		params.insert("cpus".to_owned(), json!(0));

		let err = engine
			.mesh_migrate_abort("missing-source", &base_digest, &delta_dir, &delta_digest, params)
			.expect_err("invalid create params must propagate");

		assert_eq!(err.message, "cpus must be positive");
		assert_eq!(
			cas::lookup(&base_digest).expect("base lookup after failed abort"),
			Some(base_dir.clone())
		);
		assert_eq!(
			cas::lookup(&delta_digest).expect("delta lookup after failed abort"),
			Some(delta_dir.clone())
		);
		assert!(base_dir.is_dir(), "failed migration abort must keep the pre-copy checkpoint");
		assert!(delta_dir.is_dir(), "failed migration abort must keep the delta checkpoint");
	}

	#[test]
	fn restored_s3_metadata_preserves_tags_without_persisting_credentials() {
		let mut params = Map::from_iter([(
			"s3_mounts".to_owned(),
			json!({
				"/mnt/data": {
					"uri": "s3://bucket/prefix",
					"endpoint": "http://127.0.0.1:9000",
					"region": "us-east-1",
					"read_only": false,
					"tag": "bucket",
					"auth": "anonymous"
				}
			}),
		)]);

		let tags = restore_s3_mount_params(&mut params)
			.expect("metadata parses")
			.expect("metadata carries tags");
		assert_eq!(tags.get("/mnt/data").map(String::as_str), Some("bucket"));
		assert_eq!(params["s3_mounts"]["/mnt/data"]["uri"], "s3://bucket/prefix");
		assert!(params["s3_mounts"]["/mnt/data"].get("tag").is_none());
		assert!(params["s3_mounts"]["/mnt/data"].get("auth").is_none());
		assert!(params["s3_mounts"]["/mnt/data"].get("access_key").is_none());
	}

	#[test]
	fn restored_s3_mounts_allow_null_optional_value() {
		let mut params = Map::from_iter([("s3_mounts".to_owned(), Value::Null)]);

		assert_eq!(
			restore_s3_mount_params(&mut params).expect("optional S3 mounts are accepted"),
			None
		);
	}

	#[test]
	fn same_sandbox_capture_lock_blocks_suspend_until_maintenance_releases() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let lock = engine.capture_lock("sandbox");
		let held = lock.acquire();
		let (entered_tx, entered_rx) = std::sync::mpsc::channel();
		let waiting = Arc::clone(&lock);
		let worker = std::thread::spawn(move || {
			let _guard = waiting.acquire();
			entered_tx.send(()).expect("report suspend entry");
		});

		assert!(
			entered_rx.recv_timeout(Duration::from_millis(50)).is_err(),
			"suspend entered while a keep-running maintenance capture owned the sandbox lock"
		);
		drop(held);
		entered_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("suspend enters after maintenance resumes and releases its lock");
		worker.join().expect("suspend waiter");
	}

	#[test]
	fn capture_locks_allow_different_sandboxes_to_progress_independently() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let first = engine.capture_lock("first");
		let held = first.acquire();
		let second = engine.capture_lock("second");
		let (entered_tx, entered_rx) = std::sync::mpsc::channel();
		let worker = std::thread::spawn(move || {
			let _guard = second.acquire();
			entered_tx.send(()).expect("report second capture entry");
		});

		entered_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("a capture for another sandbox must not wait on the first sandbox");
		drop(held);
		worker.join().expect("independent capture waiter");
	}

	#[test]
	fn maintenance_skips_a_sandbox_with_a_pending_lifecycle_transition() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let record = VmRecord::new("pending", "pending", "running");
		engine.insert_test_record(record);
		let transition = engine
			.begin_state_transition("pending", LifecyclePhase::Suspended)
			.expect("begin pending suspend");
		assert_eq!(transition.disposition, TransitionDisposition::Acquired);
		assert!(
			engine
				.inner
				.maintenance_busy
				.lock()
				.insert("pending".to_owned())
		);

		engine.maintenance_once();

		let record = engine.get_record("pending", false).expect("pending record");
		assert_eq!(record.lifecycle.desired, LifecyclePhase::Suspended);
		assert_eq!(record.lifecycle.observed, LifecyclePhase::Running);
		assert_eq!(record.lifecycle.generation, transition.generation);
		assert!(
			engine.inner.maintenance_busy.lock().contains("pending"),
			"maintenance must leave the pending operation owned by its transition"
		);
		engine.inner.release_maintenance("pending");
	}

	#[test]
	fn idle_policy_update_waits_for_active_maintenance() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		engine
			.inner
			.registry
			.insert_persisted(engine.home(), VmRecord::new("sandbox", "sandbox", "stopped"))
			.expect("persist record");
		assert!(
			engine
				.inner
				.maintenance_busy
				.lock()
				.insert("sandbox".to_owned())
		);
		let worker_engine = engine.clone();
		let (started_tx, started_rx) = std::sync::mpsc::channel();
		let (done_tx, done_rx) = std::sync::mpsc::channel();
		let worker = std::thread::spawn(move || {
			started_tx.send(()).expect("report start");
			done_tx
				.send(worker_engine.set_idle_timeout("sandbox", 15.0))
				.expect("report update");
		});
		started_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("idle policy updater started");
		assert!(
			done_rx.recv_timeout(Duration::from_millis(50)).is_err(),
			"idle policy update bypassed active maintenance"
		);

		engine.inner.release_maintenance("sandbox");
		let view = done_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("idle policy update finished")
			.expect("idle policy update");
		assert_eq!(view["idle_timeout_secs"], 15.0);
		worker.join().expect("idle policy updater");
	}

	#[test]
	fn rehydration_service_resolution_failure_stops_and_marks_non_runnable() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let mut record = VmRecord::new("rehydrate-failure", "rehydrate-failure", "running");
		// Names, not secret values, are the only durable credential identity.
		// Without a persisted TAP identity, gateway reconstruction must fail
		// closed instead of leaving a falsely-running sandbox.
		record.detail = json!({ "credential_names": ["tenant-token"] });
		engine.insert_test_record(record);

		engine
			.rehydrate_runtime_identities()
			.expect("rehydration failure is recorded rather than propagated");

		let record = engine
			.get_record("rehydrate-failure", false)
			.expect("record");
		assert_eq!(
			record.status, "stopped",
			"failed service reconstruction left a false running state"
		);
		assert!(
			record
				.detail
				.get("restart_non_runnable")
				.and_then(Value::as_str)
				.is_some(),
			"missing durable failure explanation: {}",
			record.detail
		);
		assert!(
			!engine
				.inner
				.runtimes
				.lock()
				.contains_key("rehydrate-failure"),
			"failed rehydration retained a live runtime candidate"
		);
	}

	fn assert_capture_permit_blocks_destructive_mutation(
		operation: impl FnOnce(&Engine) -> Result<Value> + Send + 'static,
	) {
		let temp = TempDir::new().expect("temp");
		let (engine, _runtime, _home) = snapshot_engine(&temp, usize::MAX);
		let engine = Arc::new(engine);
		engine.insert_test_record(VmRecord::new("sandbox", "sandbox", "stopped"));
		fs::create_dir_all(engine.sandbox("sandbox").dir()).expect("runtime directory");
		let lock = engine.capture_lock("sandbox");
		let held = lock.acquire();
		let (result_tx, result_rx) = std::sync::mpsc::channel();
		let worker_engine = Arc::clone(&engine);
		let worker = std::thread::spawn(move || {
			result_tx
				.send(operation(&worker_engine))
				.expect("report public mutation result");
		});

		assert!(
			result_rx.recv_timeout(Duration::from_millis(50)).is_err(),
			"public lifecycle mutation bypassed the active maintenance-capture permit"
		);
		drop(held);
		result_rx
			.recv_timeout(Duration::from_secs(1))
			.expect("public lifecycle mutation proceeds after capture release")
			.expect("immediate fake runtime operation succeeds");
		worker.join().expect("public mutation waiter");
	}

	#[test]
	fn capture_permit_blocks_stop_and_remove_until_maintenance_releases() {
		assert_capture_permit_blocks_destructive_mutation(|engine| engine.stop("sandbox"));
		assert_capture_permit_blocks_destructive_mutation(|engine| engine.remove("sandbox"));
	}

	#[test]
	fn capture_lock_map_reclaims_released_sandboxes() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		for index in 0..64 {
			drop(engine.capture_lock(&format!("sandbox-{index}")).acquire());
		}
		drop(engine.capture_lock("final").acquire());
		assert!(
			engine.test_capture_lock_count() <= 1,
			"released per-sandbox capture locks must not grow without bound"
		);
	}

	#[test]
	fn unpublished_suspend_crash_resumes_before_cancelling_the_transition() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		engine.insert_test_record(VmRecord::new("suspend-crash", "suspend-crash", "running"));
		let transition = engine
			.begin_state_transition("suspend-crash", LifecyclePhase::Suspended)
			.expect("begin suspend");
		assert_eq!(transition.disposition, TransitionDisposition::Acquired);
		let events = Arc::new(Mutex::new(Vec::new()));
		let resumed = Arc::clone(&events);
		let cancelled = Arc::clone(&events);
		engine
			.test_resume_unpublished_suspend(
				"suspend-crash",
				move || {
					resumed.lock().push("resume");
					Ok(())
				},
				move || {
					cancelled.lock().push("cancel");
					Ok(())
				},
			)
			.expect("resume and cancel unpublished suspend");

		assert_eq!(*events.lock(), vec!["resume", "cancel"]);
		let record = engine.get_record("suspend-crash", false).expect("record");
		assert_eq!(record.lifecycle.desired, LifecyclePhase::Running);
		assert_eq!(record.lifecycle.observed, LifecyclePhase::Running);
		assert_eq!(record.lifecycle.generation, transition.generation);
	}
	#[test]
	fn production_launch_specs_arm_only_the_owner_watchdog() {
		let spec = LaunchSpec::boot_rootfs("control.sock", "vmlinux", "rootfs.ext4");
		assert_eq!(
			launch_spec_for_cluster(ClusterMode::Production, &spec).owner_lease_secs,
			Some(15)
		);
		assert_eq!(launch_spec_for_cluster(ClusterMode::SingleNode, &spec).owner_lease_secs, None);
	}
	#[test]
	fn owner_lease_rearm_does_not_refresh_last_activity() {
		let temp = TempDir::new().expect("temp");
		let (engine, _runtime, _home) = snapshot_engine(&temp, usize::MAX);
		let mut record = VmRecord::new("lease", "lease", "running");
		record.last_active = 123.0;
		engine.insert_test_record(record);
		fs::create_dir_all(engine.sandbox("lease").dir()).expect("candidate runtime");

		assert!(engine.mesh_rearm_owner_lease("lease", 15).is_err());
		assert_eq!(
			engine
				.get_record("lease", false)
				.expect("record")
				.last_active,
			123.0
		);
	}

	#[test]
	fn production_ha_key_resolution_accepts_only_shared_or_owner_tenant_key() {
		let mut config = ServeConfig::default();
		config.cluster_mode = ClusterMode::Production;
		config.portable_history_key_id = Some("shared-key".to_owned());
		config.tenant_keys = std::collections::HashMap::from([
			("tenant-a".to_owned(), "tenant-a-key".to_owned()),
			("tenant-b".to_owned(), "tenant-b-key".to_owned()),
		]);

		assert_eq!(
			Engine::resolve_ha_key_id(&config, "async", "tenant-a", "default").unwrap(),
			Some("tenant-a-key".to_owned())
		);
		assert_eq!(
			Engine::resolve_ha_key_id(&config, "async", "unmapped", "").unwrap(),
			Some("shared-key".to_owned())
		);
		assert_eq!(
			Engine::resolve_ha_key_id(&config, "async", "tenant-a", "shared-key").unwrap(),
			Some("shared-key".to_owned())
		);
		assert_eq!(
			Engine::resolve_ha_key_id(&config, "async", "tenant-a", "tenant-a-key").unwrap(),
			Some("tenant-a-key".to_owned())
		);
		assert!(Engine::resolve_ha_key_id(&config, "async", "tenant-a", "host-local-key").is_err());
		assert!(Engine::resolve_ha_key_id(&config, "async", "tenant-a", "tenant-b-key").is_err());
		assert_eq!(
			Engine::resolve_ha_key_id(&config, "off", "tenant-a", "host-local-key").unwrap(),
			None
		);
	}
	#[test]
	fn paused_candidate_activation_rejects_pid_mismatch_before_resume() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let mut record = VmRecord::new("paused-candidate", "paused-candidate", "paused");
		record.pid = Some(7);
		engine.insert_test_record(record);
		write_meta(temp.path(), "paused-candidate", json!({ "pid": 8, "status": "paused" }));

		let error = engine
			.mesh_activate_candidate("paused-candidate")
			.expect_err("a stale PID must never resume a candidate");
		assert_eq!(error.code.as_str(), "busy");
		assert!(
			error.message.contains("PID no longer matches"),
			"unexpected fencing error: {}",
			error.message
		);
		assert_eq!(
			engine
				.inner
				.registry
				.get("paused-candidate")
				.expect("candidate record")
				.status,
			"paused",
			"PID fencing must retain the paused candidate for reconciliation"
		);
	}
	#[test]
	fn persistence_validation_rejects_invalid_sticky_priority() {
		let temp = TempDir::new().expect("temp");
		let (_engine, _home) = Engine::new_test(config_for(&temp));
		let mut params = valid_create();
		params.persistence = Some(PersistencePolicy::Sticky { priority: 11 });
		let error = Engine::validate_create(&params).expect_err("priority above ten");
		assert_eq!(error.code.as_str(), "invalid");
		assert!(
			serde_json::from_value::<SandboxCreate>(json!({
				"persistence": {"type": "durable"}
			}))
			.is_err(),
			"unknown persistence type must be rejected",
		);

		let error = resolve_snapshot_options(
			HashMap::from([("persistence".to_owned(), json!({"type": "sticky", "priority": 11}))]),
			None,
		)
		.err()
		.expect("restore priority above ten");
		assert_eq!(error.code.as_str(), "invalid");
	}

	#[test]
	fn network_idle_policy_honors_defaults_overrides_disabling_and_activity() {
		let temp = TempDir::new().expect("temp");
		let (_engine, _home) = Engine::new_test(config_for(&temp));
		let mut record = VmRecord::new("idle", "idle", "running");
		record.last_active = 10.0;
		record.last_network_active = 10.0;

		assert!(idle_deadline_elapsed(&record, 20.0, Some(false), 30.0));
		assert!(!idle_deadline_elapsed(&record, 20.0, Some(true), 30.0));

		record.detail = json!({
			"idle_timeout_secs": 40.0,
			"activity_threshold_bytes": 100,
		});
		assert!(!idle_deadline_elapsed(&record, 20.0, Some(false), 49.0));
		assert!(idle_deadline_elapsed(&record, 20.0, Some(true), 50.0));

		record.detail["idle_timeout_secs"] = json!(0.0);
		assert!(!idle_deadline_elapsed(&record, 20.0, Some(false), 1_000.0));

		record.detail["idle_timeout_secs"] = json!(20.0);
		record.last_network_active = 40.0;
		assert!(!idle_deadline_elapsed(&record, 20.0, Some(false), 59.0));
		assert!(idle_deadline_elapsed(&record, 20.0, Some(true), 60.0));
		assert!(!network_delta_exceeds_threshold(100, 100));
		assert!(network_delta_exceeds_threshold(101, 100));
	}

	#[test]
	fn ephemeral_suspend_policy_hook_discards_rootfs_and_recovery_state() {
		let temp = TempDir::new().expect("temp");
		let (engine, _home) = Engine::new_test(config_for(&temp));
		let mut record = VmRecord::new("ephemeral", "ephemeral", "stopped");
		record.persistence = PersistencePolicy::Ephemeral;
		let vm_dir = engine.home().vm_dir("ephemeral");
		fs::create_dir_all(&vm_dir).expect("vm dir");
		fs::write(vm_dir.join("meta.json"), b"{}").expect("meta");
		fs::write(vm_dir.join("rootfs.img"), b"stored rootfs").expect("rootfs");
		let recovery = engine.recovery_root("ephemeral").expect("recovery");
		fs::create_dir_all(&recovery).expect("recovery dir");
		fs::write(recovery.join("point.venc"), b"checkpoint").expect("checkpoint");
		engine.insert_test_record(record.clone());

		engine.apply_ephemeral_discard(&record).expect("discard");

		assert!(vm_dir.join("meta.json").exists());
		assert!(!vm_dir.join("rootfs.img").exists());
		assert!(!recovery.exists());
		assert_eq!(
			engine
				.get_record("ephemeral", false)
				.expect("record")
				.detail["state_discarded"],
			json!(true)
		);
	}

	#[test]
	fn storage_gc_evicts_lower_priority_sticky_and_never_persistent() {
		let temp = TempDir::new().expect("temp");
		let mut config = config_for(&temp);
		config.storage_quota_mb = 2;
		let (engine, _home) = Engine::new_test(config);
		for (id, persistence) in [
			("low", PersistencePolicy::Sticky { priority: 1 }),
			("high", PersistencePolicy::Sticky { priority: 9 }),
			("permanent", PersistencePolicy::Persistent),
		] {
			let mut record = VmRecord::new(id, id, "stopped");
			record.persistence = persistence;
			let dir = engine.home().vm_dir(id);
			fs::create_dir_all(&dir).expect("vm dir");
			fs::write(dir.join("meta.json"), b"{}").expect("meta");
			fs::write(dir.join("rootfs.img"), vec![0_u8; 900 * 1024]).expect("stored state");
			engine.insert_test_record(record);
		}

		engine.enforce_storage_quota().expect("storage GC");

		assert_eq!(engine.get_record("low", false).expect("low").status, "lost");
		assert_eq!(engine.get_record("high", false).expect("high").status, "stopped");
		assert_eq!(
			engine
				.get_record("permanent", false)
				.expect("persistent")
				.status,
			"stopped"
		);
		assert!(
			engine
				.home()
				.vm_dir("permanent")
				.join("rootfs.img")
				.exists()
		);
	}
}
