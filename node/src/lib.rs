//! Vibemon node role: hosts microVMs (ARCHITECTURE.md §8).
//!
//! Runs `vmon vmm` children, builds templates, stores artifacts and volumes,
//! and serves the node API (`/node/v1`) plus guest tunnels on the cluster
//! listener. [`Node`] is the facade `vmon serve --role node` drives.

pub mod api;
pub mod artifacts;
pub mod assets;
pub mod block_image;
pub mod build;
pub mod client;
pub mod config;
pub mod egress;
pub mod envd;
pub mod files;
pub mod gc4;
pub mod host;
pub mod initramfs;
pub mod lazy_resume;
pub mod logs;
pub mod metrics;
pub mod net;
pub mod oci;
pub mod paused_store;
pub mod s3fs;
pub mod sandbox;
pub mod server;
pub mod vm;
pub mod volumes;

use std::{sync::Arc, time::Duration};

use anyhow::Context;
use parking_lot::Mutex;
use tokio::task::AbortHandle;
use vmon_cluster::{Cluster, NodeCapacity};

pub use crate::config::{CapacityOverrides, NodeConfig};
use crate::{
	build::{BuildConfig, Builder},
	egress::{Egress, EgressConfig},
	files::FileStore,
	net::NetManager,
	sandbox::{ManagerConfig, SandboxManager},
	vm::{VmRuntime, VmRuntimeConfig, VmmIsolation},
	volumes::VolumeStore,
};

/// How often capacity is republished in gossip.
const CAPACITY_INTERVAL: Duration = Duration::from_secs(5);
/// How often metric retention runs.
const GC_INTERVAL: Duration = Duration::from_hours(1);

/// A running node role.
pub struct Node {
	router:       axum::Router,
	sandboxes:    Arc<SandboxManager>,
	net:          Arc<NetManager>,
	object_store: Option<vmon_cloud::s3::S3Client>,
	tasks:        Mutex<Vec<AbortHandle>>,
}

impl Node {
	/// Resolves boot assets, starts egress, the VM runtime, the builder, and
	/// the sandbox manager, adopts VMMs left by a previous process, and starts
	/// capacity publishing into `cluster` metadata.
	pub async fn start(config: NodeConfig, cluster: Cluster) -> anyhow::Result<Self> {
		std::fs::create_dir_all(&config.data_dir)
			.with_context(|| format!("creating {}", config.data_dir.display()))?;
		let http = reqwest::Client::builder()
			.connect_timeout(Duration::from_secs(5))
			.build()
			.context("building HTTP client")?;
		let kernel = assets::resolve_kernel(
			config.kernel.as_deref(),
			&config.vmon_bin,
			&config.data_dir,
			&http,
		)
		.await?;
		let agent = assets::resolve_agent(config.agent.as_deref(), &config.vmon_bin)?;
		let cpu_baseline = host::cpu_baseline(&config.vmon_bin).await?;
		let cpu_model = host::cpu_model();
		let arch = assets::host_arch().to_owned();

		let net =
			NetManager::new(config.net_mode, &config.net_pool, config.vmm_user.map(|(uid, _)| uid))?;
		// SAFETY: geteuid has no preconditions.
		let root = unsafe { libc::geteuid() } == 0;
		let isolation = match (cfg!(target_os = "linux") && root, config.vmm_user) {
			(true, Some((uid, gid))) => VmmIsolation::DropTo { uid, gid },
			(true, None) => VmmIsolation::Disabled,
			(false, _) => VmmIsolation::Default,
		};
		let boot_concurrency = if config.boot_concurrency == 0 {
			4 * host::cpus() as usize
		} else {
			config.boot_concurrency
		};
		let vms = VmRuntime::new(VmRuntimeConfig {
			vmon_bin: config.vmon_bin.clone(),
			run_dir: config.run_dir.clone(),
			boot_concurrency,
			isolation,
			net: Arc::clone(&net),
		})?;
		let egress = Egress::start(EgressConfig {
			data_dir:       config.data_dir.clone(),
			internal_allow: config.egress_internal_allow.clone(),
			proxy_port:     0,
		})
		.await
		.context("starting egress enforcement")?;
		net.set_egress(egress.clone());
		let envd_version = vmon_agent::envd::ENVD_VERSION.to_owned();
		let builder = Builder::new(
			BuildConfig {
				data_dir: config.data_dir.clone(),
				node_id: cluster.local_id(),
				arch: arch.clone(),
				backend: host::backend().to_owned(),
				cpu_baseline: cpu_baseline.clone(),
				envd_version: envd_version.clone(),
				kernel: kernel.clone(),
				agent_binary: agent,
				max_parallel: if config.max_parallel_builds == 0 {
					2
				} else {
					config.max_parallel_builds
				},
				step_cache_mb: if config.step_cache_mb == 0 {
					host::disk_size_mb(&config.data_dir) / 10
				} else {
					config.step_cache_mb
				},
				secret: config.cluster_secret.clone(),
				http,
				registry_mirror: config.registry_mirror.clone(),
				object_store: config.object_store.clone(),
			},
			vms.clone(),
		)?;
		let volumes = VolumeStore::new(config.volumes_root()).context("opening volume root")?;
		let sandboxes = SandboxManager::new(
			ManagerConfig {
				data_dir: config.data_dir.clone(),
				arch: arch.clone(),
				kernel,
				envd_version,
				run_dir: config.run_dir.clone(),
				object_store: config.object_store.clone(),
				lazy_resume: config.lazy_resume,
				s3_mounts_allow_private: config.s3_mounts_allow_private,
			},
			vms,
			builder.clone(),
			egress.clone(),
			volumes.clone(),
			Some(cluster.kv.clone()),
		)?;
		sandboxes.recover().await;
		// After recovery re-reserved adopted sandboxes' leases: idle TAPs of a
		// previous process are removed, then one ready TAP per boot slot.
		net.prewarm(boot_concurrency).await;

		let peers = server::Peers::new(cluster.clone());
		let api = server::sandbox_routes(Arc::clone(&sandboxes), peers.clone())
			.merge(build::routes(builder.clone(), peers))
			.merge(files::routes(FileStore::new(config.data_dir.join("files"))))
			.merge(volumes::routes(volumes));
		let secret: Arc<[u8]> = Arc::from(config.cluster_secret.as_bytes());
		let router = axum::Router::new()
			.nest(api::PREFIX, api)
			.layer(vmon_cluster::require_secret(secret));

		let mut features = net.features();
		features.extend(egress.features());
		if config.object_store.is_some() {
			features.push(paused_store::FEATURE.to_owned());
		}
		let capacity_task = {
			let sandboxes = Arc::clone(&sandboxes);
			let builder = builder.clone();
			let data_dir = config.data_dir.clone();
			let overrides = config.capacity;
			tokio::spawn(async move {
				let mut ticker = tokio::time::interval(CAPACITY_INTERVAL);
				loop {
					ticker.tick().await;
					let usage = sandboxes.usage();
					let capacity = NodeCapacity {
						arch:           arch.clone(),
						backend:        host::backend().to_owned(),
						os:             host::os().to_owned(),
						cpu_model:      cpu_model.clone(),
						cpu_baseline:   cpu_baseline.clone(),
						cpus:           overrides.cpus.unwrap_or_else(host::cpus),
						memory_mb:      overrides.memory_mb.unwrap_or_else(host::memory_mb),
						disk_free_mb:   host::disk_free_mb(&data_dir),
						vcpus_used:     usage.vcpus,
						memory_used_mb: usage.memory_mb,
						sandboxes:      usage.sandboxes,
						builds:         builder.running(),
						draining:       false,
						features:       features.clone(),
						cached_builds:  builder.artifacts().cached_builds(),
					};
					cluster.update_meta(|meta| {
						let draining = meta.node.as_ref().is_some_and(|node| node.draining);
						meta.node = Some(NodeCapacity { draining, ..capacity });
					});
				}
			})
			.abort_handle()
		};
		let gc_task = {
			let sandboxes = Arc::clone(&sandboxes);
			tokio::spawn(async move {
				let mut ticker = tokio::time::interval(GC_INTERVAL);
				loop {
					ticker.tick().await;
					let sandboxes = Arc::clone(&sandboxes);
					let _ = tokio::task::spawn_blocking(move || sandboxes.gc()).await;
				}
			})
			.abort_handle()
		};
		Ok(Self {
			router,
			sandboxes,
			net,
			object_store: config.object_store,
			tasks: Mutex::new(vec![capacity_task, gc_task]),
		})
	}

	/// `/node/v1/*` routes behind the cluster-secret check. Mount it on the
	/// cluster listener next to `Cluster::router()`; serve with upgrades
	/// enabled (tunnels use HTTP/1.1 `Upgrade`).
	pub fn router(&self) -> axum::Router {
		self.router.clone()
	}

	/// The operator object store (`--object-store`), if configured.
	pub const fn object_store(&self) -> Option<&vmon_cloud::s3::S3Client> {
		self.object_store.as_ref()
	}

	/// The sandbox manager (in-process callers and tests).
	pub const fn sandboxes(&self) -> &Arc<SandboxManager> {
		&self.sandboxes
	}

	/// Stops background tasks and removes idle pre-created TAPs. Sandbox VMMs
	/// keep running; the next `Node::start` on the same data dir adopts them.
	pub async fn shutdown(&self) {
		for task in self.tasks.lock().drain(..) {
			task.abort();
		}
		self.net.shutdown().await;
	}
}
