//! Template builds on the node (ARCHITECTURE.md §8.6) and artifact
//! availability for sandbox creation.
//!
//! [`Builder`] owns the build registry (status, logs, cancellation), the
//! bounded build concurrency, the OCI client, the step cache, and the
//! [`ArtifactStore`]. Builds run in VMs launched through [`VmRuntime`]; see
//! [`pipeline`] for the phases. [`Builder::ensure`] makes a build's artifacts
//! usable locally for the sandbox manager's create (peer fetch + snapshot
//! regeneration).

pub mod cache;
#[cfg(test)]
mod e2e;
pub mod log;
mod pipeline;
pub mod provision;
pub mod routes;
pub mod steps;

use std::{
	collections::HashMap,
	path::PathBuf,
	sync::{
		Arc,
		atomic::{AtomicU32, Ordering},
	},
	time::Instant,
};

use anyhow::{Context, Result, bail};
use parking_lot::Mutex;
use tokio::sync::{OnceCell, Semaphore};
use tokio_util::sync::CancellationToken;
use vmon_cluster::NodeId;

pub use self::routes::routes;
use self::{cache::StepCache, log::BuildLog};
use crate::{
	api::{ArtifactInfo, BuildReason, BuildState, BuildStatus, StartBuild},
	artifacts::{ArtifactStore, BUILD_LOG, HostIdentity},
	oci::OciClient,
	vm::VmRuntime,
};

/// Static configuration of the node's builder.
#[derive(Clone, Debug)]
pub struct BuildConfig {
	/// Node data dir (`builds/`, `cache/` live below it).
	pub data_dir:        PathBuf,
	pub node_id:         NodeId,
	/// `aarch64` | `x86_64`.
	pub arch:            String,
	/// `kvm` | `hvf` | `whp`.
	pub backend:         String,
	/// Snapshot-compatibility key (`vmon vmm --print-cpu-baseline`).
	pub cpu_baseline:    String,
	/// envd version reported by templates built here.
	pub envd_version:    String,
	/// Guest kernel image.
	pub kernel:          PathBuf,
	/// Static Linux `vmon-agent` for the guest arch.
	pub agent_binary:    PathBuf,
	/// Concurrent builds on this node.
	pub max_parallel:    usize,
	/// Cluster secret (peer artifact and file-context fetches).
	pub secret:          String,
	pub http:            reqwest::Client,
	/// Docker Hub pull-through mirror API base (`https://mirror.gcr.io`).
	pub registry_mirror: Option<String>,
	/// Operator object store: committed builds are published there and
	/// fetched from there when no peer holds them.
	pub object_store:    Option<vmon_cloud::s3::S3Client>,
}

/// Builds templates and provides artifacts. Cheap to clone.
#[derive(Clone)]
pub struct Builder {
	inner: Arc<Inner>,
}

impl std::fmt::Debug for Builder {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("Builder")
			.field("config", &self.inner.config)
			.finish_non_exhaustive()
	}
}

struct Inner {
	config:       BuildConfig,
	vms:          VmRuntime,
	store:        ArtifactStore,
	oci:          OciClient,
	cache:        StepCache,
	builds:       Mutex<HashMap<String, Arc<BuildHandle>>>,
	permits:      Semaphore,
	running:      AtomicU32,
	agent_digest: OnceCell<String>,
	initramfs:    OnceCell<PathBuf>,
}

/// One build known to this process.
struct BuildHandle {
	state:  Mutex<HandleState>,
	log:    BuildLog,
	cancel: CancellationToken,
}

struct HandleState {
	status:   BuildState,
	reason:   Option<BuildReason>,
	artifact: Option<ArtifactInfo>,
}

/// A failed build: message plus the upstream step label (`base`, `1`…,
/// `finalize`).
#[derive(Debug)]
pub struct BuildFailure {
	pub message: String,
	pub step:    Option<String>,
}

impl BuildFailure {
	fn new(step: Option<&str>, error: &anyhow::Error) -> Self {
		Self { message: format!("{error:#}"), step: step.map(str::to_owned) }
	}
}

/// Error of [`Builder::start`].
#[derive(Debug, thiserror::Error)]
pub enum StartError {
	#[error("invalid build request: {0}")]
	Invalid(String),
	#[error(transparent)]
	Internal(#[from] anyhow::Error),
}

impl Builder {
	/// Creates the builder; `vms` launches build VMs.
	pub fn new(config: BuildConfig, vms: VmRuntime) -> Result<Self> {
		let host = HostIdentity {
			node_id:      config.node_id,
			arch:         config.arch.clone(),
			backend:      config.backend.clone(),
			cpu_baseline: config.cpu_baseline.clone(),
		};
		let store =
			ArtifactStore::new(&config.data_dir, host, config.secret.clone(), config.http.clone())
				.context("opening artifact store")?
				.with_object_store(config.object_store.clone());
		let cache_dir = config.data_dir.join("cache");
		let oci = OciClient::new(
			config.http.clone(),
			&cache_dir.join("oci"),
			config.registry_mirror.as_deref(),
		);
		let cache = StepCache::new(cache_dir.join("steps")).context("opening step cache")?;
		let permits = Semaphore::new(config.max_parallel.max(1));
		Ok(Self {
			inner: Arc::new(Inner {
				config,
				vms,
				store,
				oci,
				cache,
				builds: Mutex::new(HashMap::new()),
				permits,
				running: AtomicU32::new(0),
				agent_digest: OnceCell::new(),
				initramfs: OnceCell::new(),
			}),
		})
	}

	/// The local artifact store.
	pub fn artifacts(&self) -> &ArtifactStore {
		&self.inner.store
	}

	/// Builds currently in the `building` state (for `NodeCapacity.builds`).
	pub fn running(&self) -> u32 {
		self.inner.running.load(Ordering::Relaxed)
	}

	/// Accepts a build and runs it in the background (`POST /builds`).
	/// Re-posting a known build ID is idempotent.
	pub fn start(&self, req: StartBuild) -> Result<(), StartError> {
		validate(&req).map_err(|e| StartError::Invalid(format!("{e:#}")))?;
		let dir = self
			.inner
			.store
			.build_dir(&req.build_id)
			.map_err(|e| StartError::Invalid(e.to_string()))?;
		let handle = {
			let mut builds = self.inner.builds.lock();
			if builds.contains_key(&req.build_id) || self.inner.store.local(&req.build_id).is_some() {
				return Ok(());
			}
			std::fs::create_dir_all(&dir).context("creating build dir")?;
			let handle = Arc::new(BuildHandle {
				state:  Mutex::new(HandleState {
					status:   BuildState::Waiting,
					reason:   None,
					artifact: None,
				}),
				log:    BuildLog::create(&dir.join(BUILD_LOG)),
				cancel: CancellationToken::new(),
			});
			builds.insert(req.build_id.clone(), handle.clone());
			handle
		};
		let builder = self.clone();
		tokio::spawn(async move { builder.run(req, handle).await });
		Ok(())
	}

	async fn run(self, req: StartBuild, handle: Arc<BuildHandle>) {
		let inner = &self.inner;
		let started = Instant::now();
		let permit = tokio::select! {
			permit = inner.permits.acquire() => permit.ok(),
			() = handle.cancel.cancelled() => None,
		};
		let outcome = match permit {
			None => Err(None),
			Some(_permit) => {
				handle.state.lock().status = BuildState::Building;
				inner.running.fetch_add(1, Ordering::Relaxed);
				let job = pipeline::Job::new(self.clone(), req.clone(), handle.log.clone());
				let result = tokio::select! {
					result = job.run() => result.map_err(Some),
					() = handle.cancel.cancelled() => Err(None),
				};
				inner.running.fetch_sub(1, Ordering::Relaxed);
				result
			},
		};
		let elapsed = started.elapsed();
		let mut state = handle.state.lock();
		match outcome {
			Ok(info) => {
				handle
					.log
					.info(None, format!("Build finished, took {}", pipeline::format_duration(elapsed)));
				state.status = BuildState::Ready;
				state.artifact = Some(info);
			},
			Err(Some(failure)) => {
				handle
					.log
					.error(failure.step.as_deref(), format!("Build failed: {}", failure.message));
				state.status = BuildState::Error;
				state.reason = Some(BuildReason { message: failure.message, step: failure.step });
			},
			Err(None) => {
				handle.log.error(None, "Build failed: build was cancelled");
				state.status = BuildState::Cancelled;
				state.reason =
					Some(BuildReason { message: "build was cancelled".to_owned(), step: None });
			},
		}
		if state.status != BuildState::Ready {
			pipeline::discard_outputs(&inner.store, &req.build_id);
		}
	}

	/// Status with log entries from `logs_offset` (`GET /builds/{id}`).
	/// Builds from before a node restart are answered from disk.
	pub fn status(&self, build_id: &str, logs_offset: u64) -> Option<BuildStatus> {
		let handle = self.inner.builds.lock().get(build_id).cloned();
		if let Some(handle) = handle {
			let (logs, logs_total) = handle.log.since(logs_offset);
			let state = handle.state.lock();
			return Some(BuildStatus {
				build_id: build_id.to_owned(),
				status: state.status,
				logs,
				logs_total,
				reason: state.reason.clone(),
				artifact: state.artifact.clone(),
			});
		}
		let dir = self.inner.store.build_dir(build_id).ok()?;
		let log = BuildLog::load(&dir.join(BUILD_LOG));
		let artifact = self.inner.store.local(build_id);
		if log.is_none() && artifact.is_none() {
			return None;
		}
		let (logs, logs_total) = log.map(|l| l.since(logs_offset)).unwrap_or_default();
		Some(match artifact {
			Some(info) => BuildStatus {
				build_id: build_id.to_owned(),
				status: BuildState::Ready,
				logs,
				logs_total,
				reason: None,
				artifact: Some(info),
			},
			None => BuildStatus {
				build_id: build_id.to_owned(),
				status: BuildState::Error,
				logs,
				logs_total,
				reason: Some(BuildReason {
					message: "build did not complete (node restarted)".to_owned(),
					step:    None,
				}),
				artifact: None,
			},
		})
	}

	/// Cancels a waiting or running build; the build VM is killed. Returns
	/// `false` for unknown builds.
	pub fn cancel(&self, build_id: &str) -> bool {
		let handle = self.inner.builds.lock().get(build_id).cloned();
		handle.is_some_and(|h| {
			h.cancel.cancel();
			true
		})
	}

	/// Makes `build_id` usable for sandbox creation on this node: a local
	/// hit returns immediately; otherwise artifacts are fetched from
	/// `sources`. When no compatible snapshot is available (different
	/// arch/backend/cpu baseline, or filesystem-only build) the template
	/// snapshot is regenerated locally from the rootfs. Single-flight per build.
	pub async fn ensure(&self, build_id: &str, sources: &[String]) -> Result<ArtifactInfo> {
		let store = &self.inner.store;
		let lock = store.lock(build_id);
		let _guard = lock.lock().await;
		let meta = if let Some(meta) = store.local_meta(build_id) {
			meta
		} else {
			let in_progress = self.inner.builds.lock().get(build_id).is_some_and(|h| {
				matches!(h.state.lock().status, BuildState::Waiting | BuildState::Building)
			});
			if in_progress {
				bail!("build {build_id} is still in progress on this node");
			}
			store.fetch(build_id, sources).await?
		};
		if store.host().snapshot_compatible(&meta.info) {
			return Ok(meta.info);
		}
		let log = BuildLog::create(&store.build_dir(build_id)?.join("regenerate.log"));
		pipeline::regenerate_snapshot(self, meta, &log).await
	}

	async fn agent_digest(&self) -> Result<&str> {
		let path = self.inner.config.agent_binary.clone();
		self
			.inner
			.agent_digest
			.get_or_try_init(|| async move {
				let bytes = tokio::fs::read(&path)
					.await
					.with_context(|| format!("reading agent binary {}", path.display()))?;
				Ok::<_, anyhow::Error>(crate::oci::sha256_digest(&bytes))
			})
			.await
			.map(String::as_str)
	}

	async fn initramfs(&self) -> Result<PathBuf> {
		let digest = self.agent_digest().await?.trim_start_matches("sha256:")[..16].to_owned();
		let config = &self.inner.config;
		self
			.inner
			.initramfs
			.get_or_try_init(|| async move {
				let path = config
					.data_dir
					.join("cache")
					.join(format!("unpack-initramfs-{digest}.cpio"));
				if !tokio::fs::try_exists(&path).await.unwrap_or(false) {
					let agent = tokio::fs::read(&config.agent_binary).await?;
					let image = crate::initramfs::unpack_initramfs(&agent)?;
					let tmp = path.with_extension("tmp");
					tokio::fs::write(&tmp, image).await?;
					tokio::fs::rename(&tmp, &path).await?;
				}
				Ok::<_, anyhow::Error>(path)
			})
			.await
			.cloned()
	}
}

fn validate(req: &StartBuild) -> Result<()> {
	if req.vcpus == 0 || req.memory_mb < 128 {
		bail!("vcpus must be ≥ 1 and memoryMB ≥ 128");
	}
	if req.disk_mb < 512 {
		bail!("diskMB must be ≥ 512");
	}
	if req.template_id.is_empty()
		|| !req
			.template_id
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
	{
		bail!("invalid templateID");
	}
	Ok(())
}
