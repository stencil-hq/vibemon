//! Sandbox manager (ARCHITECTURE.md §8.2).
//!
//! Create (fork the template snapshot or cold-boot its rootfs), pause/resume
//! through `paused/<id>/`, kill, deadlines, network updates,
//! snapshot-as-build, metric scraping, VMM-exit cleanup, and restart
//! recovery. Serves the `/node/v1/sandboxes` routes through `crate::server`.

mod handover;
mod meta;

use std::{
	collections::{HashMap, HashSet},
	fs,
	path::{Path, PathBuf},
	sync::{
		Arc,
		atomic::{AtomicU64, Ordering},
	},
	time::{Duration, SystemTime, UNIX_EPOCH},
};

use anyhow::{Context, anyhow};
use parking_lot::Mutex;
use tokio::task::{AbortHandle, JoinHandle};
use vmon_cloud::s3::S3Client;
use vmon_cluster::{
	NodeId,
	kv::{Compare, Kv, Op, Txn},
};

pub use self::meta::{SandboxMeta, SandboxPaths};
use crate::{
	api::{
		ArtifactInfo, CreateSandbox, GuestInit, LogEntry, LogQuery, MetricSample, NetworkPolicy,
		PauseSandbox, ResumeSandbox, S3Mount, SandboxInfo, SandboxState, SnapshotSandbox,
		VolumeMount,
	},
	artifacts::{
		self, ArtifactMeta,
		copy::{clone_disk, link_disk_base, move_disk, remove_disk},
	},
	build::{Builder, ServiceLog, start_service, steps::BuildContext},
	egress::{Egress, EgressNet},
	envd::EnvdClient,
	gc4::{AgentClient, ExecRequest, MountRequest},
	gpu::{self, GpuPool},
	lazy_resume::{self, PageServer},
	logs, metrics,
	net::NetLease,
	paused_store::{self, Cancellation, Remote, Store, UploadInput},
	s3fs::{self, S3Fs, S3FsConfig},
	server::ApiError,
	vm::{Boot, Disk, FsShare, RemoteDisk, Vm, VmGpu, VmRuntime, VmSpec, VmmError, private_dir},
	volumes::VolumeStore,
};

/// Safety margin of the VMM hard deadline past the driver's `endAt`.
const DEADLINE_SLACK: Duration = Duration::from_mins(1);
/// Readiness budget (control ping + GC4 hello) of sandbox VMs.
const READY_TIMEOUT: Duration = Duration::from_mins(1);
/// Snapshot name used for pauses inside the live snapshot root.
const PAUSE_SNAPSHOT: &str = "pause";
/// Manifest that marks a published VMM snapshot directory.
const CURRENT_GENERATION: &str = "current-generation";
/// How long a GPU VM's filesystem-only pause waits for the VMM to quit
/// before SIGKILL.
const GPU_QUIT_GRACE: Duration = Duration::from_secs(30);
/// `--volume` shares one VMM accepts.
const MAX_VOLUMES: usize = 8;

/// Current wall clock in unix ms.
pub fn now_ms() -> u64 {
	SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.map_or(0, |d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
}

/// Sandbox IDs are `i` + 20 lowercase alphanumerics; anything that is a safe,
/// short path component is accepted.
fn validate_id(id: &str) -> Result<(), ApiError> {
	if id.is_empty()
		|| id.len() > 40
		|| !id
			.bytes()
			.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
	{
		return Err(ApiError::invalid(format!("invalid sandbox id {id:?}")));
	}
	Ok(())
}

/// Seconds for the VMM `--timeout-secs`/`extend` so it fires at `end_at +
/// slack`.
fn deadline_secs(end_at_ms: u64, now_ms: u64) -> u64 {
	let target =
		end_at_ms.saturating_add(u64::try_from(DEADLINE_SLACK.as_millis()).unwrap_or(u64::MAX));
	(target.saturating_sub(now_ms) / 1000).clamp(DEADLINE_SLACK.as_secs(), 86_400)
}

/// Static inputs of the manager.
#[derive(Clone, Debug)]
pub struct ManagerConfig {
	pub data_dir:                PathBuf,
	/// Guest arch (`aarch64` | `x86_64`), for cold-boot cmdlines.
	pub arch:                    String,
	pub kernel:                  PathBuf,
	/// envd version reported when the guest does not send `X-Envd-Version`.
	pub envd_version:            String,
	/// Short directory for per-VM Unix sockets.
	pub run_dir:                 PathBuf,
	/// Operator object store: committed pauses upload there and paused
	/// sandboxes of other nodes resume from it (with the KV handle given to
	/// [`SandboxManager::new`]).
	pub object_store:            Option<S3Client>,
	/// Resume paused state from the object store lazily ([`lazy_resume`]).
	pub lazy_resume:             bool,
	/// Let bring-your-own-bucket mounts use private/loopback endpoints.
	pub s3_mounts_allow_private: bool,
	/// Host GPUs sandboxes reserve.
	pub gpus:                    Arc<GpuPool>,
}

/// A running VM and what hangs off it.
struct Live {
	vm:         Vm,
	net:        Option<NetLease>,
	/// Tasks (metric scraper, exit watcher) aborted when the VM stops.
	tasks:      Vec<AbortHandle>,
	generation: u64,
	/// Page source of a lazily restored RAM ([`lazy_resume`]): held (never
	/// read) so it serves as long as the VMM and stops when `Live` drops.
	_pages:     Option<PageServer>,
	/// GPUs the VM holds; freed once the VMM is reaped
	/// ([`SandboxManager::retire`]).
	gpus:       Vec<String>,
	/// The guest unloaded its GPU driver (`gpu_release`): the GPUs are
	/// reusable. Otherwise they are quarantined when the VMM stops.
	gpus_clean: bool,
}

impl Live {
	fn stop_tasks(&mut self) {
		for task in self.tasks.drain(..) {
			task.abort();
		}
	}
}

struct EntryState {
	meta: SandboxMeta,
	live: Option<Live>,
}

/// Background object-store work of a paused sandbox: the upload of its
/// pause, or the removal of a generation a resume consumed. Lifecycle
/// operations cancel and join it before touching its files or record.
struct Upload {
	cancel: Cancellation,
	task:   JoinHandle<()>,
}

/// One sandbox known to the node (running or paused).
struct Entry {
	paths:    SandboxPaths,
	/// Serializes lifecycle operations on this sandbox (shared so a kill can
	/// keep holding it while its teardown finishes in the background).
	ops:      Arc<tokio::sync::Mutex<()>>,
	state:    Mutex<EntryState>,
	upload:   Mutex<Option<Upload>>,
	/// Handover manifest of the paused state (`handover`); pauses drop it.
	handover: Mutex<Option<handover::CachedManifest>>,
}

/// Root disk of a [`BootPlan`].
enum PlanDisk {
	/// A private overlay of this local image.
	Overlay(PathBuf),
	/// A lazily hydrated remote block image ([`lazy_resume::prepare_disk`]).
	Remote(RemoteDisk),
}

/// A lazy cross-node resume ([`lazy_resume`]): what replaces the local
/// paused disk and, on Linux, memory.
struct LazyResume {
	disk:  RemoteDisk,
	pages: Option<PageServer>,
}

/// What [`SandboxManager::boot`] needs for one VM start.
struct BootPlan<'a> {
	boot:      Boot,
	disk:      PlanDisk,
	vcpus:     u32,
	memory_mb: u64,
	network:   &'a NetworkPolicy,
	mounts:    &'a [VolumeMount],
	/// Bring-your-own-bucket mounts ([`s3fs`]).
	s3_mounts: &'a [S3Mount],
	end_at_ms: u64,
	/// Fresh kernel boot: volumes must be mounted (a fork inherits mounts).
	cold:      bool,
	init:      &'a GuestInit,
	/// Full snapshot whose memory file the fork maps as RAM; hard-linked to
	/// [`SandboxPaths::ram_base`] so pauses only write the pages that differ.
	ram_base:  Option<PathBuf>,
	/// Page source of a [`Boot::RemotePages`] restore.
	pages:     Option<PageServer>,
	/// Build whose start/ready commands a cold boot runs.
	build_id:  &'a str,
	/// Reserved GPUs to pass through (cold boots only).
	gpus:      &'a [String],
}

/// Result of a successful boot.
struct Booted {
	vm:           Vm,
	net:          NetLease,
	envd_version: Option<String>,
	pages:        Option<PageServer>,
	gpus:         Vec<String>,
}

/// GPUs reserved for a boot: released on drop unless [`Self::keep`] hands
/// them to the running VM.
struct GpuLease<'a> {
	pool: &'a GpuPool,
	id:   &'a str,
	gpus: Vec<String>,
}

impl<'a> GpuLease<'a> {
	/// Reserves `count` GPUs for sandbox `id` (`busy` when too few are free).
	fn reserve(pool: &'a GpuPool, id: &'a str, count: u32) -> Result<Self, ApiError> {
		Ok(Self { pool, id, gpus: pool.reserve(id, count)? })
	}

	/// The boot succeeded: the VM holds the GPUs now.
	fn keep(mut self) {
		self.gpus.clear();
	}
}

impl Drop for GpuLease<'_> {
	fn drop(&mut self) {
		self.pool.release(self.id, &self.gpus);
	}
}

/// Asks the guest to unload its GPU driver right before its VMM stops (the
/// guest's processes die with it); `true` once it did.
async fn release_guest_gpus(id: &str, vm: &Vm) -> bool {
	if vm.exited().is_some() {
		return false;
	}
	match vm.agent().gpu_release().await {
		Ok(()) => true,
		Err(error) => {
			tracing::warn!(
				sandbox = id,
				error = format!("{error:#}"),
				"guest did not release its GPUs"
			);
			false
		},
	}
}

/// Owns every sandbox on this node.
pub struct SandboxManager {
	config:     ManagerConfig,
	vms:        VmRuntime,
	builder:    Builder,
	egress:     Egress,
	volumes:    VolumeStore,
	sandboxes:  Mutex<HashMap<String, Arc<Entry>>>,
	/// IDs with a create in flight.
	pending:    Mutex<HashSet<String>>,
	generation: AtomicU64,
	/// Cluster KV (paused-generation records); portable pauses need it and
	/// [`ManagerConfig::object_store`].
	kv:         Option<Kv>,
	/// Per-sandbox S3 mount proxies.
	s3fs:       S3Fs,
}

/// Removes a pending-create reservation when dropped.
struct Pending<'a> {
	manager: &'a SandboxManager,
	id:      String,
}

impl Drop for Pending<'_> {
	fn drop(&mut self) {
		self.manager.pending.lock().remove(&self.id);
	}
}

/// Resource totals of running sandboxes (capacity reporting).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Usage {
	pub sandboxes: u32,
	pub vcpus:     u32,
	pub memory_mb: u64,
}

/// Hard-links the manifest-selected generation of the VMM snapshot directory
/// `src` (`current-generation`, `vmstate.<gen>.bin`, `memory.<gen>.bin`) into
/// the new directory `dst`. Published generation files are never rewritten in
/// place, so the links stay a faithful copy of the source.
fn link_snapshot(src: &Path, dst: &Path) -> std::io::Result<()> {
	let manifest = fs::read_to_string(src.join(CURRENT_GENERATION))?;
	let generation = manifest.trim();
	if generation.is_empty() || !generation.bytes().all(|b| b.is_ascii_digit()) {
		return Err(std::io::Error::new(
			std::io::ErrorKind::InvalidData,
			format!("invalid {CURRENT_GENERATION} {generation:?}"),
		));
	}
	fs::create_dir(dst)?;
	for name in [format!("vmstate.{generation}.bin"), format!("memory.{generation}.bin")] {
		fs::hard_link(src.join(&name), dst.join(&name))?;
	}
	// Last: a manifest only exists once the files it selects do.
	fs::hard_link(src.join(CURRENT_GENERATION), dst.join(CURRENT_GENERATION))
}

/// Makes a freshly moved `paused/<id>/` durable, then publishes its commit
/// marker: the overlay disk's data, the boot init, the directory entries of
/// the renamed-in snapshot, RAM base links and disk files (the VMM already
/// fsynced the snapshot files and their directory), and finally the marker.
/// Until the marker exists the state is not resumable after a host crash, so
/// durable memory is never paired with a stale disk.
fn commit_paused(paths: &SandboxPaths) -> std::io::Result<()> {
	let sync_dir = |dir: &Path| fs::File::open(dir)?.sync_all();
	fs::File::open(paths.paused_rootfs())?.sync_data()?;
	if paths.paused_boot_init().is_file() {
		fs::File::open(paths.paused_boot_init())?.sync_data()?;
	}
	if paths.paused_base().is_dir() {
		sync_dir(&paths.paused_base())?;
	}
	sync_dir(&paths.paused)?;
	if let Some(parent) = paths.paused.parent() {
		sync_dir(parent)?;
	}
	fs::File::create(paths.paused_committed())?.sync_all()?;
	sync_dir(&paths.paused)
}

/// Scratch directory of a pause upload, beside `paused/<id>/`.
fn upload_staging(paths: &SandboxPaths) -> PathBuf {
	paths.paused.with_extension("upload")
}

/// Publishes a completed generation as `paused/<id>/remote` unless the
/// sandbox was killed, a record exists, or another node owns the pause.
async fn publish_remote(kv: &Kv, id: &str, remote: &Remote, node: NodeId) -> anyhow::Result<bool> {
	let key = paused_store::key(id);
	let txn = Txn {
		compare: vec![
			Compare::Missing(paused_store::deleted_key(id)),
			Compare::Missing(key.clone()),
			Compare::Value { key: paused_store::owner_key(id), value: serde_json::to_vec(&node)? },
		],
		success: vec![Op::Put { key, value: serde_json::to_vec(remote)?, ttl_ms: None }],
		..Txn::default()
	};
	Ok(kv.txn(txn).await?.succeeded)
}

/// Whether this node may remove the published generation of `id`: the
/// sandbox was killed (permanent fence) or its route names this node. A
/// former holder never removes the state of the current owner.
async fn owns_remote(kv: &Kv, id: &str, node: NodeId) -> anyhow::Result<bool> {
	if kv
		.get_consistent(&paused_store::deleted_key(id))
		.await?
		.is_some()
	{
		return Ok(true);
	}
	let Some(route) = kv.get_consistent(&format!("sbx/{id}")).await? else {
		return Ok(false);
	};
	let route: crate::api::SandboxRoute = serde_json::from_slice(&route.value)?;
	Ok(route.node_id == node)
}

/// API error of a failed memory pause or snapshot (`what`: `pausing`,
/// `capturing`): the VMM's nested-virtualization refusal keeps its stable
/// code, anything else is internal.
fn capture_error(id: &str, what: &str, error: &anyhow::Error) -> ApiError {
	if VmmError::has_code(error, crate::api::NESTED_VIRTUALIZATION_ACTIVE) {
		ApiError::nested_virtualization_active(id)
	} else {
		ApiError::internal(format!("{what} sandbox {id}: {error:#}"))
	}
}

fn remove_path(path: &Path) {
	let result = match fs::symlink_metadata(path) {
		Ok(meta) if meta.is_dir() => fs::remove_dir_all(path),
		Ok(_) => fs::remove_file(path),
		Err(_) => return,
	};
	if let Err(error) = result {
		tracing::warn!(path = %path.display(), %error, "cleanup failed");
	}
}

/// [`remove_disk`] with failures logged, like [`remove_path`].
fn drop_disk(path: &Path) {
	if let Err(error) = remove_disk(path) {
		tracing::warn!(path = %path.display(), %error, "disk cleanup failed");
	}
}

impl SandboxManager {
	/// Creates the manager (no sandboxes yet; call [`Self::recover`]).
	pub fn new(
		config: ManagerConfig,
		vms: VmRuntime,
		builder: Builder,
		egress: Egress,
		volumes: VolumeStore,
		kv: Option<Kv>,
	) -> anyhow::Result<Arc<Self>> {
		for dir in ["sandboxes", "paused"] {
			fs::create_dir_all(config.data_dir.join(dir))?;
		}
		handover::sweep_staging(&config.data_dir.join("paused"));
		let mut s3fs_config = S3FsConfig::new(config.run_dir.clone(), config.object_store.clone());
		s3fs_config.allow_private = config.s3_mounts_allow_private;
		let s3fs = S3Fs::new(s3fs_config);
		Ok(Arc::new(Self {
			config,
			vms,
			builder,
			egress,
			volumes,
			sandboxes: Mutex::new(HashMap::new()),
			pending: Mutex::new(HashSet::new()),
			generation: AtomicU64::new(1),
			kv,
			s3fs,
		}))
	}

	/// Object store and KV when portable pauses are enabled.
	fn portable(&self) -> Option<(&S3Client, &Kv)> {
		Some((self.config.object_store.as_ref()?, self.kv.as_ref()?))
	}

	/// Cancels and joins the entry's background object-store task.
	async fn join_upload(entry: &Entry) {
		let upload = entry.upload.lock().take();
		if let Some(upload) = upload {
			upload.cancel.cancel();
			let _ = upload.task.await;
		}
	}

	/// Removes the published generation of `id` if this node is responsible
	/// for it: the record first (a stale record must never be resumed), then
	/// (with `objects`) the objects (a failure there only leaks storage).
	async fn remove_remote(&self, id: &str, objects: bool) -> anyhow::Result<()> {
		let Some((s3, kv)) = self.portable() else {
			return Ok(());
		};
		let key = paused_store::key(id);
		let Some(record) = kv.get_consistent(&key).await? else {
			return Ok(());
		};
		if !owns_remote(kv, id, self.builder.artifacts().host().node_id).await? {
			return Ok(());
		}
		let remote: Remote = serde_json::from_slice(&record.value)?;
		kv.txn(Txn {
			compare: vec![Compare::Value { key: key.clone(), value: record.value }],
			success: vec![Op::Delete { key }],
			..Txn::default()
		})
		.await?;
		if !objects {
			return Ok(());
		}
		let deleted = async {
			Store::for_remote(Some(s3), &remote)
				.await?
				.delete(&remote)
				.await
		};
		if let Err(error) = deleted.await {
			tracing::warn!(
				sandbox = id,
				error = format!("{error:#}"),
				"removing paused objects failed"
			);
		}
		Ok(())
	}

	/// After a resume the published generation is stale: removes it (only its
	/// record without `objects`) in the background; the next lifecycle
	/// operation joins the task.
	fn retire_remote(self: &Arc<Self>, id: &str, entry: &Entry, objects: bool) {
		if self.portable().is_none() {
			return;
		}
		let (manager, id) = (Arc::clone(self), id.to_owned());
		let task = tokio::spawn(async move {
			if let Err(error) = manager.remove_remote(&id, objects).await {
				tracing::warn!(sandbox = %id, error = format!("{error:#}"), "retiring paused state failed");
			}
		});
		*entry.upload.lock() = Some(Upload { cancel: Cancellation::new(), task });
	}

	/// Uploads the committed pause of `id` and publishes its record. Errors
	/// leave the local pause usable; a cancelled or unpublished generation is
	/// removed again.
	fn start_upload(self: &Arc<Self>, id: &str, entry: &Arc<Entry>) {
		let Some((s3, kv)) = self.portable() else {
			return;
		};
		let (store, kv) = (Store::new(s3.clone()), kv.clone());
		let meta = entry.state.lock().meta.clone();
		let Some(boot_init) = self.boot_init(&entry.paths, &meta.build_id) else {
			tracing::warn!(sandbox = id, "paused sandbox has no boot init; pause stays node-local");
			return;
		};
		let artifacts = self.builder.artifacts();
		let template = artifacts.snapshot_dir(&meta.build_id).ok();
		let host = artifacts.host().clone();
		let cancel = Cancellation::new();
		let (task_cancel, task_entry, id) = (cancel.clone(), Arc::clone(entry), id.to_owned());
		let task = tokio::spawn(async move {
			let started = std::time::Instant::now();
			let staging = upload_staging(&task_entry.paths);
			// A delta may reference the template's RAM base only when that base
			// survives every node: published in the object store. Otherwise the
			// pause carries its own `base/`.
			let manifest =
				crate::artifacts::published::object_key(&meta.build_id, crate::artifacts::MANIFEST);
			let template = match store.s3().exists(&manifest).await {
				Ok(true) => template,
				Ok(false) => None,
				Err(error) => {
					tracing::warn!(sandbox = %id, %error, "checking the published template; uploading the RAM base");
					None
				},
			};
			let input = UploadInput {
				id:                &id,
				paused_dir:        &task_entry.paths.paused,
				staging:           &staging,
				meta:              &meta,
				boot_init:         &boot_init,
				host:              &host,
				template_snapshot: template.as_deref(),
			};
			let remote = match store.upload(&input, &task_cancel).await {
				Ok(remote) => remote,
				Err(error) => {
					if !task_cancel.is_cancelled() {
						tracing::warn!(sandbox = %id, error = format!("{error:#}"), "uploading paused state failed; the pause stays node-local");
					}
					return;
				},
			};
			let published = async {
				let _ops = tokio::select! {
					guard = task_entry.ops.lock() => guard,
					() = task_cancel.cancelled() => return anyhow::Ok(false),
				};
				if task_cancel.is_cancelled()
					|| task_entry.state.lock().meta.state != SandboxState::Paused
				{
					return Ok(false);
				}
				publish_remote(&kv, &id, &remote, host.node_id).await
			}
			.await;
			match published {
				Ok(true) => tracing::info!(
					sandbox = %id,
					bytes = remote.bytes,
					secs = started.elapsed().as_secs_f64(),
					manifest = %remote.manifest,
					"paused state uploaded"
				),
				result => {
					if let Err(error) = result {
						tracing::warn!(sandbox = %id, error = format!("{error:#}"), "publishing paused state failed");
					}
					if let Err(error) = store.delete(&remote).await {
						tracing::warn!(sandbox = %id, error = format!("{error:#}"), "removing unpublished paused objects failed");
					}
				},
			}
		});
		*entry.upload.lock() = Some(Upload { cancel, task });
	}

	fn paths(&self, id: &str) -> SandboxPaths {
		SandboxPaths::new(&self.config.data_dir, id)
	}

	fn entry(&self, id: &str) -> Result<Arc<Entry>, ApiError> {
		validate_id(id)?;
		self
			.sandboxes
			.lock()
			.get(id)
			.cloned()
			.ok_or_else(|| ApiError::not_found(format!("sandbox {id} not found")))
	}

	/// Host GPUs and their reservations.
	pub const fn gpus(&self) -> &Arc<GpuPool> {
		&self.config.gpus
	}

	/// Totals over running sandboxes.
	pub fn usage(&self) -> Usage {
		let sandboxes = self.sandboxes.lock();
		let mut usage = Usage::default();
		for entry in sandboxes.values() {
			let state = entry.state.lock();
			if state.live.is_some() {
				usage.sandboxes += 1;
				usage.vcpus += state.meta.vcpus;
				usage.memory_mb += state.meta.memory_mb;
			}
		}
		usage
	}

	/// All running and paused sandboxes.
	pub fn list(&self) -> Vec<SandboxInfo> {
		let mut out: Vec<_> = self
			.sandboxes
			.lock()
			.values()
			.map(|entry| entry.state.lock().meta.info())
			.collect();
		out.sort_by(|a, b| a.sandbox_id.cmp(&b.sandbox_id));
		out
	}

	/// One sandbox.
	pub fn get(&self, id: &str) -> Result<SandboxInfo, ApiError> {
		Ok(self.entry(id)?.state.lock().meta.info())
	}

	/// The running VM of a sandbox (tunnels). Paused sandboxes are a 409.
	pub fn live_vm(&self, id: &str) -> Result<Vm, ApiError> {
		let entry = self.entry(id)?;
		let state = entry.state.lock();
		state
			.live
			.as_ref()
			.map(|live| live.vm.clone())
			.ok_or_else(|| ApiError::conflict(format!("sandbox {id} is not running")))
	}

	/// Boots a VM for a sandbox: networking, volumes, launch, guest network
	/// config, egress attach, envd `/init`. Everything is undone on failure.
	async fn boot(
		&self,
		id: &str,
		paths: &SandboxPaths,
		mut plan: BootPlan<'_>,
	) -> Result<Booted, ApiError> {
		remove_path(&paths.snapshot_root());
		fs::create_dir_all(paths.snapshot_root()).map_err(ApiError::internal)?;
		let disk = match &plan.disk {
			PlanDisk::Overlay(base) => {
				drop_disk(&paths.rootfs());
				// A previous lazy run's files (its generation is gone by now).
				lazy_resume::remove_files(&paths.dir).map_err(ApiError::internal)?;
				link_disk_base(base, &paths.rootfs()).map_err(|e| {
					ApiError::internal(format!("linking the disk base {}: {e}", base.display()))
				})?;
				Disk::Overlay { base: base.clone(), path: paths.rootfs() }
			},
			// `lazy_resume::prepare_disk` laid out the files already.
			PlanDisk::Remote(remote) => Disk::Remote(remote.clone()),
		};
		if let Some(base) = &plan.ram_base
			&& let Err(error) = link_snapshot(base, &paths.ram_base())
		{
			tracing::warn!(
				sandbox = id,
				base = %base.display(),
				%error,
				"linking the RAM base failed; pauses write full memory snapshots"
			);
			remove_path(&paths.ram_base());
		}
		let mut shares = Vec::with_capacity(plan.mounts.len());
		for (index, mount) in plan.mounts.iter().enumerate() {
			if mount.object_store {
				continue;
			}
			let dir = self
				.volumes
				.dir(&mount.volume_id)
				.map_err(|_| ApiError::invalid(format!("invalid volume id {}", mount.volume_id)))?;
			if !dir.is_dir() {
				return Err(ApiError::not_found(format!(
					"volume {} is not stored on this node",
					mount.volume_id
				)));
			}
			shares.push(FsShare::Volume { tag: format!("vol{index}"), dir, read_only: false });
		}
		let gpu = if plan.gpus.is_empty() {
			None
		} else {
			// The guest agent loads the NVIDIA driver from this share.
			let bundle = self
				.config
				.gpus
				.bundle_dir()
				.ok_or_else(|| ApiError::busy("this node has no guest GPU driver bundle"))?;
			shares.push(FsShare::Volume {
				tag:       gpu::BUNDLE_TAG.to_owned(),
				dir:       bundle.to_owned(),
				read_only: true,
			});
			// Caught here: a VMM that fails to start quarantines its GPUs.
			if shares.len() > MAX_VOLUMES {
				return Err(ApiError::invalid(format!(
					"a GPU sandbox mounts at most {} volumes",
					MAX_VOLUMES - 1
				)));
			}
			Some(VmGpu {
				functions: plan
					.gpus
					.iter()
					.map(|address| self.config.gpus.sysfs_path(address))
					.collect(),
				vm_uuid:   gpu::vm_uuid(id),
			})
		};
		let s3 = self
			.s3fs
			.attach(id, &paths.dir, plan.s3_mounts, plan.mounts)
			.await
			.map_err(|e| ApiError::invalid(format!("{e:#}")))?;
		let net = match self.vms.net().acquire().await {
			Ok(net) => net,
			Err(error) => {
				self.s3fs.detach_now(id);
				return Err(ApiError::busy(format!("{error:#}")));
			},
		};
		// User-net VMMs always get a compiled filter: it blocks private ranges
		// (incl. the host via slirp) and enables live policy updates.
		let mut extra_args = if net.tap().is_none() {
			match self.egress.user_net_args(&paths.net_policy(), plan.network) {
				Ok(args) => args,
				Err(error) => {
					net.release().await;
					self.s3fs.detach_now(id);
					return Err(ApiError::invalid(format!("{error:#}")));
				},
			}
		} else {
			Vec::new()
		};
		if let Some(s3) = &s3 {
			extra_args.extend(s3.vmm_args.iter().cloned());
		}
		let spec = VmSpec {
			name: id.to_owned(),
			boot: plan.boot.clone(),
			vcpus: plan.vcpus,
			memory_mb: plan.memory_mb,
			disk: Some(disk),
			shares,
			net: net.vm_net(),
			snapshot_root: Some(paths.snapshot_root()),
			console_log: paths.console(),
			timeout: Some(Duration::from_secs(deadline_secs(plan.end_at_ms, now_ms()))),
			ready_timeout: READY_TIMEOUT,
			extra_args,
			gpu,
		};
		let vm = match self.vms.launch(spec).await {
			Ok(vm) => vm,
			Err(error) => {
				// The guest may have loaded its GPU driver before it failed.
				self.config.gpus.quarantine(id, plan.gpus);
				net.release().await;
				self.s3fs.detach_now(id);
				return Err(ApiError::internal(format!("{error:#}")));
			},
		};
		match self.prepare_guest(id, &vm, &net, &plan, s3.as_ref()).await {
			Ok(envd_version) => {
				Ok(Booted { vm, net, envd_version, pages: plan.pages.take(), gpus: plan.gpus.to_vec() })
			},
			Err(error) => {
				self.egress.detach(id).await;
				if !plan.gpus.is_empty() && !release_guest_gpus(id, &vm).await {
					self.config.gpus.quarantine(id, plan.gpus);
				}
				vm.kill().await;
				net.release().await;
				self.s3fs.detach_now(id);
				Err(ApiError::internal(format!("{error:#}")))
			},
		}
	}

	async fn prepare_guest(
		&self,
		id: &str,
		vm: &Vm,
		net: &NetLease,
		plan: &BootPlan<'_>,
		s3: Option<&s3fs::Attached>,
	) -> anyhow::Result<Option<String>> {
		let agent = vm.agent();
		if !plan.cold {
			// Restored memory carries the snapshot's CRNG state, shared by every
			// fork of it. A failure only warns: older agents lack the op.
			match agent.reseed().await {
				Ok(()) => tracing::debug!(sandbox = id, "guest CRNG reseeded"),
				Err(error) => {
					tracing::warn!(
						sandbox = id,
						error = format!("{error:#}"),
						"guest CRNG reseed failed"
					);
				},
			}
		}
		let egress_net = match net.tap() {
			Some(lease) => EgressNet::Tap {
				tap:      lease.tap.clone(),
				host_ip:  lease.host_ip,
				guest_ip: lease.guest_ip,
			},
			None => EgressNet::User { api_sock: vm.control().path().to_owned() },
		};
		// The host-side egress rules do not depend on the guest's address, so
		// they go in while the guest configures it. Both run to completion
		// (no cancellation half-way), so a failed boot's `detach` sees any
		// attachment that was made.
		let guest_net = net.guest_config();
		let (configured, attached) = tokio::join!(
			agent.net_config(&guest_net),
			self.egress.attach(id, egress_net, plan.network)
		);
		configured.context("configuring guest network")?;
		attached.context("attaching egress policy")?;
		if plan.cold {
			for (index, mount) in plan.mounts.iter().enumerate() {
				if mount.object_store {
					continue;
				}
				let request = MountRequest {
					source:    format!("vol{index}"),
					path:      mount.path.clone(),
					fstype:    "virtiofs".into(),
					read_only: false,
				};
				agent
					.mount(&request)
					.await
					.with_context(|| format!("mounting volume {} at {}", mount.name, mount.path))?;
			}
			if let Some(s3) = s3 {
				S3Fs::mount_guest(&agent, s3).await?;
			}
		}
		let mut init = plan.init.clone();
		if init.ca_bundle.is_none() {
			init.ca_bundle = self.egress.ca_bundle();
		}
		let envd_version = EnvdClient::with_token(vm.clone(), &init.envd_access_token)
			.init(&init)
			.await?;
		if plan.cold {
			self
				.start_template_service(id, &agent, plan.build_id)
				.await?;
		}
		Ok(envd_version)
	}

	/// Runs the build's start command and waits for its ready command after
	/// a cold boot, as the template snapshot did before capture: a fresh
	/// kernel runs neither (a fork inherits the running service). Applies to
	/// every cold boot (GPU sandboxes, templates without a usable snapshot,
	/// filesystem-only resumes). The service keeps running in the background.
	async fn start_template_service(
		&self,
		id: &str,
		agent: &AgentClient,
		build_id: &str,
	) -> anyhow::Result<()> {
		let Some(meta) = self.builder.artifacts().local_meta(build_id) else {
			tracing::info!(
				sandbox = id,
				build = build_id,
				"build metadata is not local; no start command after this cold boot"
			);
			return Ok(());
		};
		let has = |cmd: Option<&str>| cmd.is_some_and(|cmd| !cmd.trim().is_empty());
		if !has(meta.start_cmd.as_deref()) && !has(meta.ready_cmd.as_deref()) {
			return Ok(());
		}
		let mut ctx = BuildContext::for_artifact(&meta);
		let probe = ExecRequest {
			cmd: ["/bin/sh", "-c", "test -x /bin/bash"]
				.map(str::to_owned)
				.to_vec(),
			..ExecRequest::default()
		};
		ctx.has_bash = matches!(agent.exec(&probe).await?.output().await, Ok((_, _, 0)));
		// Dropping the output task leaves the start command running.
		drop(
			start_service(
				agent,
				&ctx,
				meta.start_cmd.as_deref(),
				meta.ready_cmd.as_deref(),
				&ServiceLog::Sandbox(id.to_owned()),
			)
			.await
			.context("starting the template's start command")?,
		);
		Ok(())
	}

	/// Spawns the metric scraper and exit watcher of a freshly booted VM.
	fn start_tasks(
		self: &Arc<Self>,
		id: &str,
		entry: &Arc<Entry>,
		vm: &Vm,
		generation: u64,
	) -> Vec<AbortHandle> {
		let token = entry.state.lock().meta.envd_access_token.clone();
		let envd = EnvdClient::with_token(vm.clone(), &token);
		let metrics_path = entry.paths.metrics();
		let scraper = tokio::spawn(async move {
			let mut ticker = tokio::time::interval(metrics::SCRAPE_INTERVAL);
			ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
			loop {
				ticker.tick().await;
				match envd.metrics().await {
					Ok(sample) => {
						if let Err(error) =
							metrics::append(&metrics_path, &metrics::sample(now_ms(), &sample))
						{
							tracing::warn!(%error, "writing metrics sample failed");
						}
					},
					Err(error) => {
						tracing::debug!(error = format!("{error:#}"), "envd metrics scrape failed");
					},
				}
			}
		})
		.abort_handle();
		let manager = Arc::clone(self);
		let id = id.to_owned();
		let watched = vm.clone();
		let watcher = tokio::spawn(async move {
			let exit = watched.wait().await;
			// Cleanup aborts this watcher (`Live::stop_tasks`): run it as its own
			// task so it is not cancelled at its first await.
			tokio::spawn(async move { manager.on_vm_exit(&id, generation, exit.reason).await });
		})
		.abort_handle();
		vec![scraper, watcher]
	}

	/// The VMM of a running sandbox exited without the node asking (guest
	/// poweroff, hard deadline, crash): release everything, mark it ended.
	async fn on_vm_exit(self: &Arc<Self>, id: &str, generation: u64, reason: Option<String>) {
		let Ok(entry) = self.entry(id) else { return };
		let _ops = Arc::clone(&entry.ops).lock_owned().await;
		let live = {
			let mut state = entry.state.lock();
			if state
				.live
				.as_ref()
				.is_none_or(|live| live.generation != generation)
			{
				return;
			}
			state.live.take()
		};
		tracing::warn!(
			sandbox = id,
			reason = reason.as_deref().unwrap_or("unknown"),
			"sandbox VMM exited"
		);
		if let Some(live) = live {
			let _ = self.retire(id, live).await.await;
		}
		self.s3fs.detach(id).await;
		let (manager, id) = (Arc::clone(self), id.to_owned());
		let _ = tokio::task::spawn_blocking(move || manager.finish(&id, &entry)).await;
	}

	/// Takes a VM out of service without waiting for host teardown: aborts its
	/// tasks, SIGKILLs the VMM, and forgets its egress attachment (a resume can
	/// attach again right away). The returned, already running task releases
	/// the egress rules, reaps the VMM, and then frees the TAP lease and the
	/// GPUs (quarantined unless the guest released them, see
	/// [`release_guest_gpus`]); it never touches the sandbox's paths or entry.
	async fn retire(&self, id: &str, mut live: Live) -> JoinHandle<()> {
		live.stop_tasks();
		live.vm.terminate();
		let attachment = self.egress.take(id).await;
		let egress = self.egress.clone();
		let gpus = Arc::clone(&self.config.gpus);
		let id = id.to_owned();
		tokio::spawn(async move {
			let rules = async {
				if let Some(net) = &attachment {
					egress.release(net).await;
				}
			};
			tokio::join!(rules, live.vm.wait());
			if live.gpus_clean {
				gpus.release(&id, &live.gpus);
			} else {
				gpus.quarantine(&id, &live.gpus);
			}
			if let Some(net) = live.net.take() {
				net.release().await;
			}
		})
	}

	/// Marks a sandbox ended: drops its disks and paused state, keeps
	/// console/metrics for retention, forgets it. Blocking file I/O.
	fn finish(&self, id: &str, entry: &Entry) {
		lazy_resume::release(self.config.object_store.as_ref(), &entry.paths.dir);
		self.finish_local(id, entry);
	}

	/// [`Self::finish`] without ending a lazily resumed disk's object-store
	/// generation.
	fn finish_local(&self, id: &str, entry: &Entry) {
		s3fs::forget_mounts(&entry.paths.dir);
		drop_disk(&entry.paths.rootfs());
		remove_path(&entry.paths.snapshot_root());
		remove_path(&entry.paths.net_policy());
		remove_path(&entry.paths.paused);
		remove_path(&upload_staging(&entry.paths));
		{
			let mut state = entry.state.lock();
			state.meta.ended_at_ms = Some(now_ms());
			state.meta.pid = None;
			state.meta.tap = None;
			state.meta.gpus.clear();
			if let Err(error) = meta::save(&entry.paths, &state.meta) {
				tracing::warn!(sandbox = id, %error, "saving ended sandbox meta failed");
			}
		}
		self.forget(id, entry);
	}

	/// Drops `entry` from the table unless the ID now names another entry.
	fn forget(&self, id: &str, entry: &Entry) {
		let mut sandboxes = self.sandboxes.lock();
		if sandboxes
			.get(id)
			.is_some_and(|current| std::ptr::eq(current.as_ref(), entry))
		{
			sandboxes.remove(id);
		}
	}

	fn install_live(self: &Arc<Self>, id: &str, entry: &Arc<Entry>, booted: Booted) {
		let generation = self.generation.fetch_add(1, Ordering::Relaxed);
		let tasks = self.start_tasks(id, entry, &booted.vm, generation);
		let mut state = entry.state.lock();
		state.meta.pid = Some(booted.vm.pid());
		state.meta.tap = booted.net.tap().cloned();
		state.meta.gpus.clone_from(&booted.gpus);
		state.live = Some(Live {
			vm: booted.vm,
			net: Some(booted.net),
			tasks,
			generation,
			_pages: booted.pages,
			gpus: booted.gpus,
			gpus_clean: false,
		});
	}

	/// `POST /sandboxes`; `sources` are the cluster URLs of
	/// `req.artifact_sources`.
	pub async fn create(
		self: &Arc<Self>,
		req: CreateSandbox,
		sources: &[String],
	) -> Result<SandboxInfo, ApiError> {
		validate_id(&req.sandbox_id)?;
		let id = req.sandbox_id.clone();
		if self.sandboxes.lock().contains_key(&id) || !self.pending.lock().insert(id.clone()) {
			return Err(ApiError::conflict(format!("sandbox {id} already exists")));
		}
		let _pending = Pending { manager: self, id: id.clone() };
		// Before the artifact: a node without free GPUs answers `busy` at once.
		let gpus = GpuLease::reserve(&self.config.gpus, &id, req.gpus)?;
		let artifact = self
			.builder
			.ensure(&req.build_id, sources)
			.await
			.map_err(|e| {
				ApiError::busy(format!("build {} is not available on this node: {e:#}", req.build_id))
			})?;
		let store = self.builder.artifacts();
		let base = store
			.rootfs_path(&req.build_id)
			.map_err(|e| ApiError::invalid(format!("{e:#}")))?;
		let paths = self.paths(&id);
		remove_path(&paths.dir);
		remove_path(&paths.paused);
		remove_path(&upload_staging(&paths));
		private_dir(&paths.dir).map_err(ApiError::internal)?;
		// A fork inherits the snapshot's device set: it has no node-local
		// volumes, and its S3 device tags (recorded beside snapshots captured
		// with mounts, e.g. fork templates) must be exactly the requested ones.
		// GPU state cannot be restored: GPU sandboxes always boot fresh.
		let snapshot = (!req.cold_boot && req.gpus == 0 && artifact.has_snapshot)
			.then(|| store.snapshot_dir(&req.build_id))
			.transpose()
			.map_err(ApiError::internal)?;
		let s3_tags: Vec<String> = s3fs::plan(&req.s3_mounts, &req.volume_mounts)
			.into_iter()
			.map(|mount| mount.tag)
			.collect();
		let snapshot = snapshot.filter(|snapshot| {
			!req.volume_mounts.iter().any(|volume| !volume.object_store)
				&& s3fs::snapshot_tags(snapshot) == s3_tags
		});
		let fork = snapshot.is_some();
		let (boot, ram_base) = match snapshot {
			Some(snapshot) => (Boot::Fork { snapshot: snapshot.clone() }, Some(snapshot)),
			None => (self.cold_boot(&paths, &req.build_id)?, None),
		};
		let plan = BootPlan {
			boot,
			disk: PlanDisk::Overlay(base),
			vcpus: req.vcpus,
			memory_mb: req.memory_mb,
			network: &req.network,
			mounts: &req.volume_mounts,
			s3_mounts: &req.s3_mounts,
			end_at_ms: req.end_at_ms,
			cold: !fork,
			init: &req.init,
			ram_base,
			pages: None,
			build_id: &req.build_id,
			gpus: &gpus.gpus,
		};
		let booted = match self.boot(&id, &paths, plan).await {
			Ok(booted) => booted,
			Err(error) => {
				drop_disk(&paths.rootfs());
				remove_path(&paths.snapshot_root());
				return Err(error);
			},
		};
		let meta = SandboxMeta {
			sandbox_id:        id.clone(),
			team_id:           req.team_id,
			template_id:       req.template_id,
			build_id:          req.build_id,
			state:             SandboxState::Running,
			vcpus:             if fork { artifact.vcpus } else { req.vcpus },
			memory_mb:         if fork {
				artifact.memory_mb
			} else {
				req.memory_mb
			},
			disk_mb:           req.disk_mb,
			started_at_ms:     now_ms(),
			end_at_ms:         req.end_at_ms,
			envd_version:      booted
				.envd_version
				.clone()
				.unwrap_or_else(|| artifact.envd_version.clone()),
			envd_access_token: req.init.envd_access_token.clone(),
			network:           req.network,
			volume_mounts:     req.volume_mounts,
			pid:               None,
			tap:               None,
			ended_at_ms:       None,
			gpus:              Vec::new(),
		};
		let entry = Arc::new(Entry {
			paths,
			ops: Arc::default(),
			state: Mutex::new(EntryState { meta, live: None }),
			upload: Mutex::default(),
			handover: Mutex::default(),
		});
		self.install_live(&id, &entry, booted);
		gpus.keep();
		let info = {
			let state = entry.state.lock();
			if let Err(error) = meta::save_unsynced(&entry.paths, &state.meta) {
				tracing::warn!(sandbox = %id, %error, "saving sandbox meta failed");
			}
			state.meta.info()
		};
		self.sandboxes.lock().insert(id, entry);
		Ok(info)
	}

	/// Cold boot of a sandbox's rootfs with its build's `init`
	/// ([`Self::boot_init`]).
	fn cold_boot(&self, paths: &SandboxPaths, build_id: &str) -> Result<Boot, ApiError> {
		let init = self.boot_init(paths, build_id).ok_or_else(|| {
			ApiError::not_found(format!("build {build_id} has no local artifacts to boot"))
		})?;
		Ok(Boot::Cold {
			kernel:  self.config.kernel.clone(),
			initrd:  None,
			cmdline: artifacts::kernel_cmdline(&self.config.arch, &init),
		})
	}

	/// Kernel `init=` of a sandbox's build: the copy its pause carries
	/// ([`SandboxPaths::paused_boot_init`]), else the local build's. A sandbox
	/// resumed on a node without its build (from the object store, after a
	/// handover) only has the carried copy.
	fn boot_init(&self, paths: &SandboxPaths, build_id: &str) -> Option<String> {
		fs::read(paths.paused_boot_init())
			.ok()
			.and_then(|bytes| serde_json::from_slice(&bytes).ok())
			.or_else(|| self.builder.artifacts().boot_init(build_id))
	}

	/// `POST /sandboxes/{id}/pause`: memory snapshot (or filesystem only)
	/// into `paused/<id>/`, then the VMM exits. A fork writes its memory as a
	/// delta against its hard-linked RAM base, which moves along to
	/// `paused/<id>/base`. The VMM is reaped and its network torn down after
	/// the reply. With an object store the committed pause then uploads in
	/// the background ([`paused_store`]), unless it is the source pause of a
	/// handover (`req.handover`), which a peer pulls from this node instead.
	pub async fn pause(
		self: &Arc<Self>,
		id: &str,
		req: &PauseSandbox,
	) -> Result<SandboxInfo, ApiError> {
		let PauseSandbox { memory, handover } = *req;
		let entry = self.entry(id)?;
		let holds_gpus = !entry.state.lock().meta.gpus.is_empty();
		if memory && holds_gpus {
			return Err(ApiError::gpu_memory_snapshot(id));
		}
		// Held until the paused state is durable (see `commit_paused`), so a
		// resume or kill waits for it instead of racing it.
		let ops = Arc::clone(&entry.ops).lock_owned().await;
		entry.handover.lock().take();
		// A previous generation must be gone before this pause can publish.
		Self::join_upload(&entry).await;
		// A lazily resumed disk still reads its generation's objects.
		self
			.remove_remote(id, !lazy_resume::is_remote(&entry.paths.dir))
			.await
			.map_err(|error| ApiError::busy(format!("removing stale paused state: {error:#}")))?;
		let vm = entry
			.state
			.lock()
			.live
			.as_ref()
			.map(|live| live.vm.clone())
			.ok_or_else(|| ApiError::conflict(format!("sandbox {id} is not running")))?;
		let paths = &entry.paths;
		let delta = memory && paths.ram_base().join(CURRENT_GENERATION).is_file();
		let build_id = entry.state.lock().meta.build_id.clone();
		// Read before the previous pause's dir goes: the next one carries it on.
		let boot_init = self.boot_init(paths, &build_id);
		let captured = async {
			if memory {
				vm.pause().await?;
				// Write-back mounts: the bucket must hold every guest write
				// before the pause counts (resume may happen on another node).
				self.s3fs.sync(id).await?;
				if delta {
					vm.snapshot_delta(PAUSE_SNAPSHOT, meta::RAM_BASE).await
				} else {
					vm.snapshot(PAUSE_SNAPSHOT).await
				}
			} else {
				// The guest unloads its GPU driver first (its GPU users die), so
				// the GPUs are reusable once the VMM is gone.
				if holds_gpus
					&& release_guest_gpus(id, &vm).await
					&& let Some(live) = entry.state.lock().live.as_mut()
				{
					live.gpus_clean = true;
				}
				// Flush guest page cache, then stop all writes.
				if let Err(error) = vm.agent().fs_sync().await {
					tracing::warn!(
						sandbox = id,
						error = format!("{error:#}"),
						"fs_sync before filesystem pause failed"
					);
				}
				// The VMM refuses to pause with a VFIO GPU attached, so a GPU VM
				// (driver unloaded, filesystem synced) quits instead: the VMM
				// closes its disk overlay, persisting the allocation bitmap that
				// a SIGKILL would leave stale (losing the latest writes).
				if holds_gpus {
					vm.shutdown(GPU_QUIT_GRACE).await;
				} else {
					vm.pause().await?;
				}
				self.s3fs.sync(id).await
			}
		}
		.await;
		// A lazily resumed disk becomes one local image again while the VM is
		// still paused (its files are quiescent): fetches the blocks the guest
		// never touched.
		let materialized = paths.dir.join("rootfs.materialized");
		let captured = match captured {
			Ok(()) if lazy_resume::is_remote(&paths.dir) => {
				drop_disk(&materialized);
				let started = std::time::Instant::now();
				let result = lazy_resume::materialize_disk(
					self.config.object_store.as_ref(),
					&paths.dir,
					&materialized,
				)
				.await;
				if result.is_ok() {
					tracing::info!(
						sandbox = id,
						secs = started.elapsed().as_secs_f64(),
						"materialized the lazily resumed disk"
					);
				} else {
					remove_path(&paths.snapshot_root().join(PAUSE_SNAPSHOT));
				}
				result
			},
			other => other,
		};
		if let Err(error) = captured {
			// A failed pause kills the VMM; a failed snapshot leaves it paused.
			if vm.exited().is_none() {
				let _ = vm.resume().await;
			}
			return Err(capture_error(id, "pausing", &error));
		}
		let live = entry.state.lock().live.take();
		if let Some(live) = live {
			// The VM is paused with every device quiesced and its state
			// captured; nothing is left to flush, so skip the `quit` round trip
			// and leave reaping and network teardown to the background. GPUs
			// are free only once the VMM is reaped: wait for that.
			let teardown = self.retire(id, live).await;
			if holds_gpus {
				let _ = teardown.await;
			}
		}
		// Uploads finished above; the resume attaches a fresh proxy.
		self.s3fs.detach_now(id);
		let moved = (|| -> std::io::Result<()> {
			remove_path(&paths.paused);
			fs::create_dir_all(&paths.paused)?;
			if let Some(init) = &boot_init {
				fs::write(paths.paused_boot_init(), serde_json::to_vec(init)?)?;
			}
			if memory {
				fs::rename(paths.snapshot_root().join(PAUSE_SNAPSHOT), paths.paused_snapshot())?;
				if delta {
					fs::rename(paths.ram_base(), paths.paused_base())?;
				}
			}
			if materialized.is_file() {
				move_disk(&materialized, &paths.paused_rootfs())
			} else {
				move_disk(&paths.rootfs(), &paths.paused_rootfs())
			}
		})();
		remove_path(&paths.snapshot_root());
		if moved.is_ok() {
			// A remote disk is local again: its files and generation go.
			lazy_resume::release(self.config.object_store.as_ref(), &paths.dir);
		}
		let mut state = entry.state.lock();
		if let Err(error) = moved {
			drop(state);
			drop_disk(&materialized);
			self.finish(id, &entry);
			return Err(ApiError::internal(format!("storing paused state of {id}: {error}")));
		}
		state.meta.state = SandboxState::Paused;
		state.meta.pid = None;
		state.meta.tap = None;
		state.meta.gpus.clear();
		meta::save(paths, &state.meta).map_err(ApiError::internal)?;
		let info = state.meta.info();
		drop(state);
		// Guest flushes do not fsync sandbox overlays: make the pause durable
		// off the request path, then commit it and start its upload (a
		// handover's target pulls it instead).
		let (paths, id) = (paths.clone(), id.to_owned());
		let (manager, entry) = (Arc::clone(self), Arc::clone(&entry));
		tokio::task::spawn_blocking(move || {
			match commit_paused(&paths) {
				Ok(()) if handover => {},
				Ok(()) => manager.start_upload(&id, &entry),
				Err(error) => tracing::warn!(
					sandbox = id,
					%error,
					"committing paused state failed; it is lost if the host crashes"
				),
			}
			drop(ops);
		});
		Ok(info)
	}

	/// Registers a paused sandbox this node does not know yet, adopting the
	/// driver's metadata for a resume from the object store. Returns the
	/// entry and whether it is new.
	fn adopt_remote(&self, id: &str, req: &ResumeSandbox) -> Result<(Arc<Entry>, bool), ApiError> {
		if self.portable().is_none() {
			return Err(ApiError::invalid("this node has no object store for portable paused state"));
		}
		let mut meta = req
			.meta
			.clone()
			.ok_or_else(|| ApiError::invalid("a remote resume needs the sandbox metadata"))?;
		if meta.sandbox_id != id || meta.state != SandboxState::Paused {
			return Err(ApiError::invalid("remote resume metadata does not match the sandbox"));
		}
		meta.pid = None;
		meta.tap = None;
		meta.ended_at_ms = None;
		meta.gpus.clear();
		meta
			.envd_access_token
			.clone_from(&req.init.envd_access_token);
		let mut sandboxes = self.sandboxes.lock();
		if let Some(entry) = sandboxes.get(id) {
			return Ok((Arc::clone(entry), false));
		}
		if self.pending.lock().contains(id) {
			return Err(ApiError::conflict(format!("sandbox {id} is being created")));
		}
		let entry = Arc::new(Entry {
			paths:    self.paths(id),
			ops:      Arc::default(),
			state:    Mutex::new(EntryState { meta, live: None }),
			upload:   Mutex::default(),
			handover: Mutex::default(),
		});
		sandboxes.insert(id.to_owned(), Arc::clone(&entry));
		Ok((entry, true))
	}

	/// Fetches the published paused state into `paused/<id>/` and commits it.
	/// A cold boot skips the memory snapshot. A delta snapshot whose RAM base
	/// is the template snapshot gets it linked from the local build.
	///
	/// With `lazy_resume` (and a block-image rootfs) it first tries a lazy
	/// resume instead: everything but the rootfs (and, on Linux, the memory
	/// files) is fetched, and the returned [`LazyResume`] serves the rest on
	/// demand; the paused dir is then not committed (it only feeds this
	/// boot). Any lazy failure falls back to the full fetch.
	async fn fetch_remote(
		&self,
		id: &str,
		remote: &Remote,
		req: &ResumeSandbox,
		sources: &[String],
		paths: &SandboxPaths,
		allow_lazy: bool,
	) -> Result<Option<LazyResume>, ApiError> {
		let (s3, _) = self.portable().ok_or_else(|| {
			ApiError::invalid("this node has no object store for portable paused state")
		})?;
		let started = std::time::Instant::now();
		let store = Store::for_remote(Some(s3), remote)
			.await
			.map_err(|error| ApiError::busy(format!("{error:#}")))?;
		private_dir(&paths.dir).map_err(ApiError::internal)?;
		let cold = req.cold_boot;
		if self.config.lazy_resume && allow_lazy {
			match self.fetch_lazy(&store, remote, cold, sources, paths).await {
				Ok(Some(lazy)) => {
					tracing::info!(
						sandbox = id,
						secs = started.elapsed().as_secs_f64(),
						cold,
						lazy_memory = lazy.pages.is_some(),
						"prepared a lazy resume from the object store"
					);
					return Ok(Some(lazy));
				},
				Ok(None) => {},
				Err(error) => tracing::warn!(
					sandbox = id,
					error = format!("{error:#}"),
					"lazy resume unavailable; fetching the whole paused state"
				),
			}
			if let Err(error) = lazy_resume::remove_files(&paths.dir) {
				tracing::warn!(sandbox = id, %error, "removing lazy resume files failed");
			}
		}
		remove_path(&paths.paused);
		let manifest = store
			.fetch(remote, &paths.paused, |path| {
				!(cold && (path.starts_with("snapshot/") || path.starts_with("base/")))
			})
			.await
			.map_err(|error| ApiError::busy(format!("fetching paused state of {id}: {error:#}")))?;
		if !cold {
			self.link_ram_base(&manifest, sources, paths).await?;
		}
		let committed = paths.clone();
		tokio::task::spawn_blocking(move || commit_paused(&committed))
			.await
			.map_err(ApiError::internal)?
			.map_err(ApiError::internal)?;
		tracing::info!(
			sandbox = id,
			bytes = remote.bytes,
			secs = started.elapsed().as_secs_f64(),
			cold,
			"fetched paused state from the object store"
		);
		Ok(None)
	}

	/// The lazy half of [`Self::fetch_remote`]: `None` when the generation
	/// has no block-image rootfs.
	async fn fetch_lazy(
		&self,
		store: &Store,
		remote: &Remote,
		cold: bool,
		sources: &[String],
		paths: &SandboxPaths,
	) -> anyhow::Result<Option<LazyResume>> {
		let manifest = store.manifest(remote).await?;
		if !manifest
			.files
			.iter()
			.any(|file| file.path == crate::block_image::IMAGE)
		{
			return Ok(None);
		}
		remove_path(&paths.paused);
		let manifest = store
			.fetch(remote, &paths.paused, |path| {
				lazy_resume::fetch_filter(path)
					&& !(cold && (path.starts_with("snapshot/") || path.starts_with("base/")))
			})
			.await?;
		if !cold {
			self
				.link_ram_base(&manifest, sources, paths)
				.await
				.map_err(|error| anyhow!("{}", error.message))?;
		}
		let generation = store.generation_prefix(remote)?;
		let disk = lazy_resume::prepare_disk(store.s3(), &generation, &paths.dir).await?;
		let pages = if lazy_resume::LAZY_MEMORY
			&& !cold
			&& paths.paused_snapshot().join(CURRENT_GENERATION).is_file()
		{
			let cache = paths.dir.join(lazy_resume::PAGES);
			Some(
				lazy_resume::page_server(
					store.s3(),
					&generation,
					&manifest.files,
					&paths.paused,
					&cache,
				)
				.await?,
			)
		} else {
			None
		};
		Ok(Some(LazyResume { disk, pages }))
	}

	/// Links the template snapshot as `paused/<id>/base` when a fetched delta
	/// snapshot's RAM base is the template's
	/// ([`paused_store::PausedManifest::ram_base_build`]).
	async fn link_ram_base(
		&self,
		manifest: &paused_store::PausedManifest,
		sources: &[String],
		paths: &SandboxPaths,
	) -> Result<(), ApiError> {
		let Some(build) = &manifest.ram_base_build else {
			return Ok(());
		};
		self
			.builder
			.ensure(build, sources)
			.await
			.map_err(|error| ApiError::busy(format!("template RAM base {build}: {error:#}")))?;
		let template = self
			.builder
			.artifacts()
			.snapshot_dir(build)
			.map_err(ApiError::internal)?;
		remove_path(&paths.paused_base());
		link_snapshot(&template, &paths.paused_base()).map_err(ApiError::internal)
	}

	/// `POST /sandboxes/{id}/resume`: fork the paused memory snapshot (or
	/// cold-boot the paused rootfs) and re-run envd `/init`. Paused state
	/// this node does not hold is fetched from `req.remote` first; `sources`
	/// are the cluster URLs of `req.artifact_sources`.
	pub async fn resume(
		self: &Arc<Self>,
		id: &str,
		mut req: ResumeSandbox,
		sources: &[String],
	) -> Result<SandboxInfo, ApiError> {
		validate_id(id)?;
		// GPU state cannot be restored: the paused filesystem boots fresh.
		req.cold_boot |= req.gpus > 0;
		if let Some(remote) = &req.remote {
			let generation = paused_store::generation_prefix(remote)
				.map_err(|error| ApiError::invalid(format!("{error:#}")))?;
			if !generation.starts_with(&format!("paused/{id}/")) {
				return Err(ApiError::invalid("the paused generation belongs to another sandbox"));
			}
		}
		let (entry, adopted) = match self.entry(id) {
			Ok(entry) => (entry, false),
			Err(error) if req.remote.is_none() => return Err(error),
			Err(_) => self.adopt_remote(id, &req)?,
		};
		let ops = Arc::clone(&entry.ops).lock_owned().await;
		// Type-erased: the resume future is deep enough to overflow `Send`
		// inference of the axum handler.
		let resumed: std::pin::Pin<
			Box<dyn Future<Output = Result<SandboxInfo, ApiError>> + Send + '_>,
		> = Box::pin(self.resume_locked(id, &entry, &req, sources));
		let result = resumed.await;
		if result.is_err() && adopted {
			// Nothing of the adopted sandbox stays behind on a failed resume.
			self.forget(id, &entry);
			remove_path(&entry.paths.paused);
		}
		if result.is_err()
			&& lazy_resume::is_remote(&entry.paths.dir)
			&& let Err(error) = lazy_resume::remove_files(&entry.paths.dir)
		{
			tracing::warn!(sandbox = id, %error, "removing lazy resume files failed");
		}
		drop(ops);
		result
	}

	async fn resume_locked(
		self: &Arc<Self>,
		id: &str,
		entry: &Arc<Entry>,
		req: &ResumeSandbox,
		sources: &[String],
	) -> Result<SandboxInfo, ApiError> {
		Self::join_upload(entry).await;
		let (build_id, vcpus, memory_mb) = {
			let state = entry.state.lock();
			if state.live.is_some() || state.meta.state != SandboxState::Paused {
				return Err(ApiError::conflict(format!("sandbox {id} is not paused")));
			}
			(state.meta.build_id.clone(), state.meta.vcpus, state.meta.memory_mb)
		};
		// Before fetching anything: a node without free GPUs answers `busy`.
		let gpus = GpuLease::reserve(&self.config.gpus, id, req.gpus)?;
		let paths = entry.paths.clone();
		let lazy = if paths.paused_resumable() {
			None
		} else {
			let remote = req
				.remote
				.as_ref()
				.ok_or_else(|| ApiError::internal(format!("paused state of {id} is missing")))?;
			// A failed boot may quarantine GPUs, so GPU sandboxes skip the lazy
			// attempt and its retry.
			self
				.fetch_remote(id, remote, req, sources, &paths, req.gpus == 0)
				.await?
		};
		let lazily = lazy.is_some();
		let booted = match self
			.boot_paused(id, &paths, req, &build_id, vcpus, memory_mb, lazy, &gpus.gpus)
			.await
		{
			Err(error) if lazily => {
				// The lazy path is an optimization: retry with the whole state.
				tracing::warn!(
					sandbox = id,
					error = error.message,
					"lazy resume failed to boot; fetching the whole paused state"
				);
				if let Err(error) = lazy_resume::remove_files(&paths.dir) {
					tracing::warn!(sandbox = id, %error, "removing lazy resume files failed");
				}
				let remote = req
					.remote
					.as_ref()
					.ok_or_else(|| ApiError::internal(format!("paused state of {id} is missing")))?;
				self
					.fetch_remote(id, remote, req, sources, &paths, false)
					.await?;
				self
					.boot_paused(id, &paths, req, &build_id, vcpus, memory_mb, None, &gpus.gpus)
					.await?
			},
			result => result?,
		};
		let lazily = lazily && lazy_resume::is_remote(&paths.dir);
		{
			let mut state = entry.state.lock();
			state.meta.state = SandboxState::Running;
			state.meta.end_at_ms = req.end_at_ms;
			state.meta.network = req.network.clone();
			state.meta.volume_mounts.clone_from(&req.volume_mounts);
			state
				.meta
				.envd_access_token
				.clone_from(&req.init.envd_access_token);
			if let Some(version) = &booted.envd_version {
				state.meta.envd_version.clone_from(version);
			}
		}
		self.install_live(id, entry, booted);
		gpus.keep();
		let info = {
			let state = entry.state.lock();
			meta::save(&paths, &state.meta).map_err(ApiError::internal)?;
			state.meta.info()
		};
		// A lazy disk still reads the generation: only its record goes now; the
		// objects go once the disk is local again (`lazy_resume::release`).
		self.retire_remote(id, entry, !lazily);
		Ok(info)
	}

	/// Boots the paused state in `paths.paused` (or, for a lazy resume, its
	/// metadata plus `lazy`'s remote disk and pages).
	#[allow(clippy::too_many_arguments, reason = "one call site per resume attempt")]
	async fn boot_paused(
		&self,
		id: &str,
		paths: &SandboxPaths,
		req: &ResumeSandbox,
		build_id: &str,
		vcpus: u32,
		memory_mb: u64,
		lazy: Option<LazyResume>,
		gpus: &[String],
	) -> Result<Booted, ApiError> {
		let fork = !req.cold_boot && paths.paused_snapshot().join(CURRENT_GENERATION).is_file();
		let (disk, pages) = match lazy {
			Some(lazy) => (PlanDisk::Remote(lazy.disk), lazy.pages),
			None => (PlanDisk::Overlay(paths.paused_rootfs()), None),
		};
		let (boot, ram_base) = if let (true, Some(server)) = (fork, &pages) {
			// Lazily paged RAM is anonymous: no RAM base (the next pause is a
			// full snapshot).
			(
				Boot::RemotePages {
					snapshot: paths.paused_snapshot(),
					pages:    server.pages().clone(),
				},
				None,
			)
		} else if fork {
			// A delta pause maps its base's memory file; a full one its own.
			let ram_base = if paths.paused_base().join(CURRENT_GENERATION).is_file() {
				paths.paused_base()
			} else {
				paths.paused_snapshot()
			};
			(Boot::Fork { snapshot: paths.paused_snapshot() }, Some(ram_base))
		} else {
			(self.cold_boot(paths, build_id)?, None)
		};
		let plan = BootPlan {
			boot,
			disk,
			vcpus,
			memory_mb,
			network: &req.network,
			mounts: &req.volume_mounts,
			s3_mounts: &req.s3_mounts,
			end_at_ms: req.end_at_ms,
			cold: !fork,
			init: &req.init,
			ram_base,
			pages,
			build_id,
			gpus,
		};
		self.boot(id, paths, plan).await
	}

	/// `DELETE /sandboxes/{id}`: SIGKILLs the VMM and forgets the sandbox
	/// (capacity, listing) at once. Host teardown (egress rules, TAP, disks,
	/// paused state) finishes in the background while still holding the
	/// sandbox's operation lock; after a node restart [`Self::recover`]
	/// cleans up whatever it did not reach.
	pub async fn kill(self: &Arc<Self>, id: &str) -> Result<(), ApiError> {
		let entry = self.entry(id)?;
		let ops = if let Ok(ops) = Arc::clone(&entry.ops).try_lock_owned() {
			ops
		} else {
			// An operation on the running VM holds the lock; a snapshot it
			// waits for has no deadline and can stall on host I/O. Killing the
			// VMM first makes that operation fail and release the lock.
			let vm = entry.state.lock().live.as_ref().map(|live| live.vm.clone());
			if let Some(vm) = vm {
				vm.terminate();
			}
			Arc::clone(&entry.ops).lock_owned().await
		};
		let live = entry.state.lock().live.take();
		self.forget(id, &entry);
		// A GPU VM first unloads its guest driver (bounded), off the request
		// path; any other VM stops at once.
		let (gpu_live, teardown) = match live {
			Some(live) if !live.gpus.is_empty() => (Some(live), None),
			Some(live) => (None, Some(self.retire(id, live).await)),
			None => (None, None),
		};
		let (manager, id) = (Arc::clone(self), id.to_owned());
		tokio::spawn(async move {
			if let Some(mut live) = gpu_live {
				live.gpus_clean |= release_guest_gpus(&id, &live.vm).await;
				let _ = manager.retire(&id, live).await.await;
			}
			if let Some(teardown) = teardown {
				let _ = teardown.await;
			}
			// Write-back changes the guest already made still reach the bucket.
			manager.s3fs.detach(&id).await;
			// The upload reads the paused files `finish` deletes.
			Self::join_upload(&entry).await;
			if let Err(error) = manager.remove_remote(&id, true).await {
				tracing::warn!(sandbox = %id, error = format!("{error:#}"), "removing killed paused state failed");
			}
			let _ = tokio::task::spawn_blocking(move || manager.finish(&id, &entry)).await;
			drop(ops);
		});
		Ok(())
	}

	/// `POST /sandboxes/{id}/deadline`: records `endAt` and moves the VMM hard
	/// deadline to `endAt + 60 s`.
	pub async fn set_deadline(&self, id: &str, end_at_ms: u64) -> Result<(), ApiError> {
		let entry = self.entry(id)?;
		let vm = {
			let mut state = entry.state.lock();
			state.meta.end_at_ms = end_at_ms;
			meta::save(&entry.paths, &state.meta).map_err(ApiError::internal)?;
			state.live.as_ref().map(|live| live.vm.clone())
		};
		if let Some(vm) = vm {
			vm.control()
				.extend(deadline_secs(end_at_ms, now_ms()))
				.await
				.map_err(ApiError::from)?;
		}
		Ok(())
	}

	/// `PUT /sandboxes/{id}/network`.
	pub async fn update_network(&self, id: &str, policy: NetworkPolicy) -> Result<(), ApiError> {
		let entry = self.entry(id)?;
		let _ops = entry.ops.lock().await;
		let running = entry.state.lock().live.is_some();
		if running {
			self
				.egress
				.update(id, &policy)
				.await
				.map_err(|e| ApiError::invalid(format!("{e:#}")))?;
		}
		let mut state = entry.state.lock();
		state.meta.network = policy;
		meta::save(&entry.paths, &state.meta).map_err(ApiError::internal)
	}

	/// `POST /sandboxes/{id}/snapshot`: pause, capture memory (optional) and
	/// a frozen rootfs copy, resume, and install both as build `req.build_id`.
	pub async fn snapshot(&self, id: &str, req: SnapshotSandbox) -> Result<ArtifactInfo, ApiError> {
		let entry = self.entry(id)?;
		if req.memory && !entry.state.lock().meta.gpus.is_empty() {
			return Err(ApiError::gpu_memory_snapshot(id));
		}
		let _ops = entry.ops.lock().await;
		let (vm, meta) = {
			let state = entry.state.lock();
			let vm = state
				.live
				.as_ref()
				.map(|live| live.vm.clone())
				.ok_or_else(|| ApiError::conflict(format!("sandbox {id} is not running")))?;
			(vm, state.meta.clone())
		};
		let store = self.builder.artifacts();
		store
			.build_dir(&req.build_id)
			.map_err(|e| ApiError::invalid(format!("{e:#}")))?;
		let source = store.local_meta(&meta.build_id).ok_or_else(|| {
			ApiError::not_found(format!("source build {} has no local artifacts", meta.build_id))
		})?;
		let snapshot_name = format!("build-{}", req.build_id);
		let snapshot_dir = entry.paths.snapshot_root().join(&snapshot_name);
		let rootfs_copy = entry.paths.dir.join(format!("build-{}.ext4", req.build_id));
		remove_path(&snapshot_dir);
		drop_disk(&rootfs_copy);
		if !req.memory {
			// Filesystem-only capture: make guest writes durable first.
			let _ = vm.agent().fs_sync().await;
		}
		vm.pause().await.map_err(ApiError::from)?;
		let captured = async {
			// Builds from this snapshot (fork children) read the mounts' data.
			self.s3fs.sync(id).await?;
			if req.memory {
				vm.snapshot(&snapshot_name).await?;
				// Forks of it must bind the same S3 device tags.
				s3fs::save_snapshot_tags(&snapshot_dir, &self.s3fs.tags(id))?;
			}
			if lazy_resume::is_remote(&entry.paths.dir) {
				// A lazily resumed disk: write it whole (the VM is paused).
				lazy_resume::materialize_disk(
					self.config.object_store.as_ref(),
					&entry.paths.dir,
					&rootfs_copy,
				)
				.await?;
			} else {
				let (src, dst) = (entry.paths.rootfs(), rootfs_copy.clone());
				tokio::task::spawn_blocking(move || clone_disk(&src, &dst)).await??;
			}
			anyhow::Ok(())
		}
		.await;
		let resumed = vm.resume().await;
		if resumed.is_ok()
			&& let Err(error) = vm.agent().reseed().await
		{
			// The source keeps running even if its kernel cannot reseed.
			tracing::warn!(
				sandbox = id,
				error = format!("{error:#}"),
				"guest CRNG reseed after snapshot failed"
			);
		}
		let cleanup = || {
			remove_path(&snapshot_dir);
			drop_disk(&rootfs_copy);
		};
		if let Err(error) = captured {
			cleanup();
			return Err(capture_error(id, "capturing", &error));
		}
		if let Err(error) = resumed {
			tracing::error!(
				sandbox = id,
				error = format!("{error:#}"),
				"resume after snapshot failed"
			);
		}
		// The source build's defaults, boot init, start/ready commands and
		// agent carry over: cold boots and snapshot regeneration start the
		// snapshot's service like the source's.
		let build = ArtifactMeta {
			info: ArtifactInfo {
				build_id: req.build_id.clone(),
				bytes: 0,
				envd_version: meta.envd_version.clone(),
				vcpus: meta.vcpus,
				memory_mb: meta.memory_mb,
				disk_mb: meta.disk_mb,
				..source.info
			},
			template_id: req.template_id.clone(),
			..source
		};
		let snapshot = req.memory.then_some(snapshot_dir.as_path());
		match store
			.install_snapshot_build(&req.build_id, snapshot, &rootfs_copy, build)
			.await
		{
			Ok(info) => Ok(info),
			Err(error) => {
				cleanup();
				Err(ApiError::internal(format!("installing build {}: {error:#}", req.build_id)))
			},
		}
	}

	/// Sandbox dir of a known or recently ended sandbox (metrics/logs outlive
	/// kills).
	fn history_paths(&self, id: &str) -> Result<(SandboxPaths, u64), ApiError> {
		validate_id(id)?;
		if let Some(entry) = self.sandboxes.lock().get(id) {
			return Ok((entry.paths.clone(), entry.state.lock().meta.started_at_ms));
		}
		let paths = self.paths(id);
		let meta = meta::load(&paths.meta())
			.ok_or_else(|| ApiError::not_found(format!("sandbox {id} not found")))?;
		Ok((paths, meta.started_at_ms))
	}

	/// `GET /sandboxes/{id}/metrics`.
	pub async fn metrics(
		&self,
		id: &str,
		start_ms: Option<u64>,
		end_ms: Option<u64>,
	) -> Result<Vec<MetricSample>, ApiError> {
		let (paths, _) = self.history_paths(id)?;
		tokio::task::spawn_blocking(move || metrics::read_range(&paths.metrics(), start_ms, end_ms))
			.await
			.map_err(ApiError::internal)?
			.map_err(ApiError::internal)
	}

	/// `GET /sandboxes/{id}/logs`.
	pub async fn logs(&self, id: &str, query: LogQuery) -> Result<Vec<LogEntry>, ApiError> {
		let (paths, started) = self.history_paths(id)?;
		tokio::task::spawn_blocking(move || logs::read(&paths.console(), started, &query))
			.await
			.map_err(ApiError::internal)?
			.map_err(ApiError::internal)
	}

	/// Restart recovery: re-adopt live VMMs from `sandboxes/*/meta.json`,
	/// keep paused sandboxes, and clean up sandboxes whose VMM is gone.
	pub async fn recover(self: &Arc<Self>) {
		let Ok(dirs) = fs::read_dir(self.config.data_dir.join("sandboxes")) else {
			return;
		};
		for dir in dirs.filter_map(Result::ok) {
			let Some(id) = dir.file_name().to_str().map(str::to_owned) else {
				continue;
			};
			if validate_id(&id).is_err() {
				continue;
			}
			let paths = self.paths(&id);
			let Some(meta) = meta::load(&paths.meta()) else {
				continue;
			};
			if meta.ended_at_ms.is_some() {
				continue;
			}
			let entry = Arc::new(Entry {
				paths:    paths.clone(),
				ops:      Arc::default(),
				state:    Mutex::new(EntryState { meta: meta.clone(), live: None }),
				upload:   Mutex::default(),
				handover: Mutex::default(),
			});
			match meta.state {
				SandboxState::Paused if paths.paused_resumable() => {
					tracing::info!(sandbox = %id, "recovered paused sandbox");
					self.sandboxes.lock().insert(id.clone(), Arc::clone(&entry));
					self.resume_upload(&id, &entry);
				},
				SandboxState::Paused => {
					tracing::warn!(
						sandbox = %id,
						"dropping paused sandbox whose state never became durable"
					);
					self.finish(&id, &entry);
				},
				SandboxState::Running => match self.adopt(&id, &meta).await {
					Ok((vm, net)) => {
						// The live VMM still holds its GPUs.
						if let Err(error) = self.config.gpus.claim(&id, &meta.gpus) {
							tracing::error!(sandbox = %id, %error, "re-reserving the GPUs of an adopted sandbox failed");
						}
						if let Err(error) = self.reattach_egress(&id, &vm, &net, &meta.network).await {
							tracing::warn!(sandbox = %id, error = format!("{error:#}"), "re-attaching egress failed");
						}
						// The VMM reconnects to its S3 proxy socket lazily.
						if let Err(error) = self.s3fs.reattach(&id, &paths.dir).await {
							tracing::warn!(sandbox = %id, error = format!("{error:#}"), "re-serving S3 mounts failed");
						}
						// A lazily restored RAM's page source died with the old node
						// process (see `lazy_resume`): such a VM fails its next
						// remote page fault.
						self.install_live(&id, &entry, Booted {
							vm,
							net,
							envd_version: None,
							pages: None,
							gpus: meta.gpus.clone(),
						});
						tracing::info!(sandbox = %id, "adopted running sandbox");
						self.sandboxes.lock().insert(id, entry);
					},
					Err(error) => {
						tracing::warn!(sandbox = %id, error = format!("{error:#}"), "dropping sandbox whose VMM is gone");
						if let Some(pid) = meta.pid
							&& let Ok(pid) = libc::pid_t::try_from(pid)
						{
							// SAFETY: kill(2) on the recorded VMM pid; failure is
							// harmless.
							unsafe { libc::kill(pid, libc::SIGKILL) };
						}
						// Its guest never released them.
						self.config.gpus.quarantine(&id, &meta.gpus);
						if let Ok(net) = self.vms.net().adopt(meta.tap.clone()) {
							net.release().await;
						}
						self.finish(&id, &entry);
					},
				},
			}
		}
	}

	/// Restarts the upload of a recovered pause this node still owns when no
	/// generation was published before the restart. Runs in the background
	/// (the cluster KV may not be reachable yet) under the sandbox's lock.
	fn resume_upload(self: &Arc<Self>, id: &str, entry: &Arc<Entry>) {
		if self.portable().is_none() {
			return;
		}
		let (manager, id, entry) = (Arc::clone(self), id.to_owned(), Arc::clone(entry));
		tokio::spawn(async move {
			let Some((_, kv)) = manager.portable() else {
				return;
			};
			let node = manager.builder.artifacts().host().node_id;
			for _ in 0..60 {
				let ops = entry.ops.lock().await;
				if entry.state.lock().meta.state != SandboxState::Paused
					|| entry.upload.lock().is_some()
					|| !manager
						.entry(&id)
						.is_ok_and(|current| Arc::ptr_eq(&current, &entry))
				{
					return;
				}
				let pending = async {
					if kv.get_consistent(&paused_store::key(&id)).await?.is_some() {
						return anyhow::Ok(false);
					}
					let owner = kv.get_consistent(&paused_store::owner_key(&id)).await?;
					Ok(owner.is_some_and(|owner| {
						serde_json::from_slice::<NodeId>(&owner.value).ok() == Some(node)
					}))
				};
				match pending.await {
					Ok(true) => return manager.start_upload(&id, &entry),
					Ok(false) => return,
					Err(error) => {
						tracing::debug!(sandbox = %id, error = format!("{error:#}"), "checking the paused upload");
					},
				}
				drop(ops);
				tokio::time::sleep(Duration::from_secs(5)).await;
			}
			tracing::warn!(sandbox = %id, "gave up restarting the paused upload: cluster KV unreachable");
		});
	}

	async fn adopt(&self, id: &str, meta: &SandboxMeta) -> anyhow::Result<(Vm, NetLease)> {
		let pid = meta.pid.ok_or_else(|| anyhow!("no recorded VMM pid"))?;
		let net = self.vms.net().adopt(meta.tap.clone())?;
		match self.vms.adopt(id, pid, self.paths(id).console()).await {
			Ok(vm) => Ok((vm, net)),
			Err(error) => {
				net.release().await;
				Err(error)
			},
		}
	}

	async fn reattach_egress(
		&self,
		id: &str,
		vm: &Vm,
		net: &NetLease,
		policy: &NetworkPolicy,
	) -> anyhow::Result<()> {
		let egress_net = match net.tap() {
			Some(lease) => EgressNet::Tap {
				tap:      lease.tap.clone(),
				host_ip:  lease.host_ip,
				guest_ip: lease.guest_ip,
			},
			None => EgressNet::User { api_sock: vm.control().path().to_owned() },
		};
		self.egress.attach(id, egress_net, policy).await
	}

	/// Retention: prunes metric files and deletes sandbox dirs that ended (or
	/// never recorded metadata) more than 7 days ago.
	pub fn gc(&self) {
		let now = now_ms();
		let retention = u64::try_from(metrics::RETENTION.as_millis()).unwrap_or(u64::MAX);
		let Ok(dirs) = fs::read_dir(self.config.data_dir.join("sandboxes")) else {
			return;
		};
		for dir in dirs.filter_map(Result::ok) {
			let path = dir.path();
			if retired(&path, now, retention) {
				remove_path(&path);
			} else if let Err(error) = metrics::prune(&path.join("metrics.jsonl"), now) {
				tracing::debug!(path = %path.display(), %error, "metrics prune failed");
			}
		}
	}
}

/// Whether the sandbox dir `path` is past `retention`: ended that long ago,
/// or without readable metadata (a create that never finished, or unsynced
/// metadata lost with the host, see [`meta::save_unsynced`]) and unchanged
/// that long.
fn retired(path: &Path, now_ms: u64, retention_ms: u64) -> bool {
	let since = match meta::load(&path.join("meta.json")) {
		Some(meta) => meta.ended_at_ms,
		None => fs::metadata(path)
			.and_then(|meta| meta.modified())
			.ok()
			.and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
			.and_then(|age| u64::try_from(age.as_millis()).ok()),
	};
	since.is_some_and(|since| now_ms.saturating_sub(since) > retention_ms)
}

#[cfg(test)]
mod tests {
	use super::*;

	/// Recovery resumes a paused sandbox only once its commit marker exists,
	/// which `commit_paused` writes after the disk and directory entries are
	/// durable.
	#[test]
	fn paused_state_is_resumable_only_after_commit() {
		let data = tempfile::tempdir().unwrap();
		let paths = SandboxPaths::new(data.path(), "icommittest");
		fs::create_dir_all(paths.paused_snapshot()).unwrap();
		fs::create_dir_all(paths.paused_base()).unwrap();
		fs::write(paths.paused_rootfs(), b"disk").unwrap();
		assert!(!paths.paused_resumable(), "a crash before the commit loses the pause");

		commit_paused(&paths).unwrap();
		assert!(paths.paused_resumable());

		fs::remove_file(paths.paused_rootfs()).unwrap();
		assert!(!paths.paused_resumable(), "a marker without the disk is not resumable");
		assert!(commit_paused(&paths).is_err(), "nothing to commit without the disk");
	}

	/// A sandbox dir whose metadata never became readable (a create cut
	/// short, or unsynced metadata lost with the host) ages out like an ended
	/// one; running sandboxes and fresh dirs stay.
	#[test]
	fn gc_retires_ended_and_metadata_less_dirs() {
		const DAY_MS: u64 = 86_400_000;
		let data = tempfile::tempdir().unwrap();
		let now = now_ms();
		let paths = SandboxPaths::new(data.path(), "igctest");
		fs::create_dir_all(&paths.dir).unwrap();
		assert!(!retired(&paths.dir, now, DAY_MS), "a create may still be booting");
		fs::File::open(&paths.dir)
			.unwrap()
			.set_modified(SystemTime::now() - Duration::from_hours(48))
			.unwrap();
		assert!(retired(&paths.dir, now, DAY_MS));

		let mut meta = SandboxMeta {
			sandbox_id:        "igctest".into(),
			team_id:           "team".into(),
			template_id:       "tpl".into(),
			build_id:          "build".into(),
			state:             SandboxState::Running,
			vcpus:             1,
			memory_mb:         256,
			disk_mb:           512,
			started_at_ms:     now - 3 * DAY_MS,
			end_at_ms:         now + DAY_MS,
			envd_version:      String::new(),
			envd_access_token: String::new(),
			network:           NetworkPolicy::default(),
			volume_mounts:     Vec::new(),
			pid:               None,
			tap:               None,
			ended_at_ms:       None,
			gpus:              Vec::new(),
		};
		meta::save_unsynced(&paths, &meta).unwrap();
		fs::File::open(&paths.dir)
			.unwrap()
			.set_modified(SystemTime::now() - Duration::from_hours(48))
			.unwrap();
		assert!(!retired(&paths.dir, now, DAY_MS), "running sandboxes stay");
		meta.ended_at_ms = Some(now - DAY_MS / 2);
		meta::save(&paths, &meta).unwrap();
		assert!(!retired(&paths.dir, now, DAY_MS));
		assert!(retired(&paths.dir, now + DAY_MS, DAY_MS));
	}

	#[test]
	fn deadline_keeps_slack_and_bounds() {
		let now = 1_000_000;
		// endAt in 10 minutes → fires 11 minutes from now.
		assert_eq!(deadline_secs(now + 600_000, now), 660);
		// endAt already passed → never below the slack.
		assert_eq!(deadline_secs(now - 5_000, now), 60);
		// Beyond the VMM's 24 h cap.
		assert_eq!(deadline_secs(now + 3 * 86_400_000, now), 86_400);
	}

	#[test]
	fn sandbox_ids_must_be_short_lowercase_tokens() {
		assert!(validate_id("i0123456789abcdefghij").is_ok());
		for bad in ["", "I0123", "a-b", "../x", "a/b", &"a".repeat(41)] {
			assert!(validate_id(bad).is_err(), "{bad:?}");
		}
	}
}
