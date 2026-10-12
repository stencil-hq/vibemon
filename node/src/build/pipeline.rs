//! Build phases (ARCHITECTURE.md §8.6).
//!
//! Phases: base (pull + unpack VM + provisioning), default-user phase
//! (template version 2.1), steps, and finalize (configure, start/ready,
//! snapshot).
//!
//! VMs:
//! - unpack VM: kernel + generated initramfs (agent as `/init`), target disk as
//!   `/dev/vda`, layer blobs over a read-only virtio-fs share; GC4 `mount` +
//!   `oci_unpack` + `provision` + `poweroff`.
//! - steps VM: the rootfs with the agent as PID 1 (`init=/usr/bin/envd`) and
//!   network; runs provisioning, the default-user phase, and the steps.
//! - template VM: the rootfs with its own init (`/usr/sbin/init`) and network,
//!   no shares; runs configure, start/ready, then pause + snapshot.

use std::{
	collections::BTreeSet,
	path::{Path, PathBuf},
	time::{Duration, Instant},
};

use anyhow::{Context, Result, anyhow, bail};
use futures::TryStreamExt;
use serde_json::json;
use tokio::io::AsyncWriteExt;
use vmon_agent::proto::Op;

use super::{
	BuildFailure, Builder,
	cache::{self, CopyPolicy, LayerDef},
	log::{BuildLog, LineSplitter},
	provision,
	steps::{self, BuildContext, CONTEXT_MOUNT, CopySpec, Step},
};
use crate::{
	api::{ArtifactInfo, BuildBase, GuestInit, StartBuild, TemplateVersion},
	artifacts::{
		ArtifactMeta, ArtifactStore, MANIFEST, META, ROOTFS, SNAPSHOT, copy::materialize_disk,
		kernel_cmdline, now_ms,
	},
	client::NodeClient,
	envd::EnvdClient,
	gc4::{AgentClient, ExecEvent, ExecRequest, MountRequest},
	oci::{self, ResolvedImage},
	vm::{Boot, BuildNet, Disk, FsShare, Vm, VmSpec},
};

const UNPACK_READY: Duration = Duration::from_mins(1);
const STEPS_READY: Duration = Duration::from_mins(2);
const TEMPLATE_READY: Duration = Duration::from_mins(5);
/// VMM hard deadline for any build VM.
const VM_DEADLINE: Duration = Duration::from_hours(3);
const OCI_UNPACK_TIMEOUT: Duration = Duration::from_mins(30);
const PROVISION_TIMEOUT: Duration = Duration::from_mins(20);
const CONFIGURE_TIMEOUT: Duration = Duration::from_mins(5);
const COMMAND_TIMEOUT: Duration = Duration::from_hours(1);
const SCRIPT_TIMEOUT: Duration = Duration::from_mins(5);
const READY_TIMEOUT: Duration = Duration::from_mins(10);
const READY_INTERVAL: Duration = Duration::from_secs(2);
const POWEROFF_GRACE: Duration = Duration::from_mins(1);
/// Waits (bounded) until the guest init finished starting its boot services,
/// so the template snapshot captures a settled system rather than one in the
/// middle of boot work (host-key generation, service start-up).
const SETTLE_INIT: &str = r"if [ -d /run/systemd/system ]; then
	timeout 120 systemctl is-system-running --wait >/dev/null 2>&1 || true
elif command -v rc-status >/dev/null 2>&1; then
	i=0
	while pgrep -f '[o]penrc default' >/dev/null 2>&1 && [ $i -lt 600 ]; do sleep 0.2; i=$((i+1)); done
fi
";
/// Minimum resources for helper VMs (unpack/steps); the template VM uses the
/// request.
const HELPER_MEMORY_MB: u64 = 1024;
const HELPER_VCPUS: u32 = 2;
/// virtio-fs tags.
const LAYERS_TAG: &str = "layers";
const CONTEXT_TAG: &str = "context";
const LAYERS_MOUNT: &str = "/mnt/layers";
const UNPACK_ROOT: &str = "/mnt/root";

const BASE: &str = "base";
const FINALIZE: &str = "finalize";

/// One template build.
pub(super) struct Job {
	builder: Builder,
	req:     StartBuild,
	log:     BuildLog,
}

/// A planned layer with its log label and header text.
struct LayerPlan {
	key:    String,
	forced: bool,
	/// `step` label (`base`, `1`…).
	label:  String,
	/// Header text without hash, e.g. `[builder 2/3] RUN make`.
	text:   String,
	action: Action,
}

#[derive(Clone)]
enum Action {
	/// Base materialization (handled separately).
	Base,
	/// The template-version-2.1 default user phase.
	DefaultUser,
	Step(Step, Option<String>),
}

enum BaseSource {
	Image { image: String, resolved: Box<ResolvedImage> },
	Template { build_id: String, meta: Box<ArtifactMeta> },
}

/// Kills a build VM (and releases its network) if the build is dropped or
/// fails.
struct VmGuard {
	vm:  Option<Vm>,
	net: Option<BuildNet>,
}

impl VmGuard {
	const fn vm(&self) -> &Vm {
		self.vm.as_ref().expect("guard holds a vm until finished")
	}

	/// Kills the VM and waits for it (used once a paused VM has been
	/// snapshotted: its disk is quiescent and nothing remains to flush).
	async fn kill(mut self) {
		if let Some(vm) = self.vm.take() {
			vm.kill().await;
		}
		self.net.take();
	}

	/// Waits for a guest-initiated power-off, killing the VM after `grace`.
	async fn wait_poweroff(mut self, grace: Duration) -> Result<()> {
		let Some(vm) = self.vm.take() else {
			return Ok(());
		};
		let result = tokio::time::timeout(grace, vm.wait()).await;
		if result.is_err() {
			vm.kill().await;
			bail!("vm {} did not power off within {grace:?}", vm.name());
		}
		self.net.take();
		Ok(())
	}
}

impl Drop for VmGuard {
	fn drop(&mut self) {
		let Some(vm) = self.vm.take() else { return };
		let net = self.net.take();
		if vm.exited().is_some() {
			return;
		}
		if let Ok(handle) = tokio::runtime::Handle::try_current() {
			handle.spawn(async move {
				vm.kill().await;
				drop(net);
			});
		}
	}
}

fn fail(step: &str) -> impl FnOnce(anyhow::Error) -> BuildFailure + '_ {
	move |error| BuildFailure::new(Some(step), &error)
}

impl Job {
	pub(super) const fn new(builder: Builder, req: StartBuild, log: BuildLog) -> Self {
		Self { builder, req, log }
	}

	pub(super) async fn run(self) -> Result<ArtifactInfo, BuildFailure> {
		let Self { builder, req, log } = self;
		let inner = &builder.inner;
		let store = &inner.store;
		let cfg = &inner.config;
		let started = Instant::now();
		log.info(None, format!("Building template {}/{}", req.template_id, req.build_id));
		let build_dir = store.build_dir(&req.build_id).map_err(fail(BASE))?;
		let rootfs = build_dir.join(ROOTFS);

		let parsed: Vec<Step> = req
			.steps
			.iter()
			.enumerate()
			.map(|(i, step)| {
				Step::parse(step).map_err(|e| BuildFailure::new(Some(&(i + 1).to_string()), &e))
			})
			.collect::<Result<_, _>>()?;
		let agent_digest = builder.agent_digest().await.map_err(fail(BASE))?.to_owned();

		// Base source and its cache key.
		let (base, base_identity, base_text) = match &req.from {
			BuildBase::Image { image, registry } => {
				let resolved = inner
					.oci
					.resolve(image, registry.as_ref(), &cfg.arch)
					.await
					.map_err(fail(BASE))?;
				let identity = resolved.manifest_digest.clone();
				(
					BaseSource::Image { image: image.clone(), resolved: Box::new(resolved) },
					identity,
					format!("[base] FROM {image}"),
				)
			},
			BuildBase::Build { build_id, sources } => {
				let meta = parent_meta(store, build_id, sources)
					.await
					.map_err(fail(BASE))?;
				(
					BaseSource::Template { build_id: build_id.clone(), meta: Box::new(meta) },
					format!("template:{build_id}"),
					format!("[base] FROM TEMPLATE {build_id}"),
				)
			},
		};
		let base_key =
			cache::base_key(&base_identity, req.disk_mb, &provision::fingerprint(), &agent_digest);

		// Layer plan: base, optional default-user phase, steps.
		let user_args = vec!["user".to_owned(), "true".to_owned()];
		let mut defs = Vec::new();
		let mut actions = Vec::new();
		if req.template_version == TemplateVersion::V2_1 {
			defs.push(LayerDef {
				kind:       "USER",
				args:       &user_args,
				files_hash: None,
				force:      false,
			});
			actions.push((
				BASE.to_owned(),
				"[base] DEFAULT USER user".to_owned(),
				Action::DefaultUser,
			));
		}
		let total = req.steps.len();
		for (i, (step, parsed)) in req.steps.iter().zip(&parsed).enumerate() {
			defs.push(LayerDef {
				kind:       &step.kind,
				args:       &step.args,
				files_hash: step.files_hash.as_deref(),
				force:      step.force,
			});
			actions.push((
				(i + 1).to_string(),
				format!("[builder {}/{total}] {}", i + 1, steps::describe(step)),
				Action::Step(parsed.clone(), step.files_hash.clone()),
			));
		}
		let planned = cache::plan(base_key, req.force, &defs);
		let mut layers: Vec<LayerPlan> = Vec::with_capacity(planned.len());
		for (index, layer) in planned.into_iter().enumerate() {
			let (label, text, action) = if index == 0 {
				(BASE.to_owned(), base_text.clone(), Action::Base)
			} else {
				actions[index - 1].clone()
			};
			layers.push(LayerPlan { key: layer.key, forced: layer.forced, label, text, action });
		}
		let chain: Vec<cache::Layer> = layers
			.iter()
			.map(|l| cache::Layer { key: l.key.clone(), forced: l.forced })
			.collect();
		let resume = cache::resume_point(&chain, |key| inner.cache.contains(key));
		let last = layers.len() - 1;

		// Materialize the starting rootfs.
		let base_started = Instant::now();
		let mut needs_provision = false;
		let mut refresh_agent = false;
		let (mut ctx, start) = match (resume, &base) {
			(Some(index), _) => {
				for layer in &layers[..=index] {
					log.info(Some(&layer.label), format!("CACHED {} [{}]", layer.text, layer.key));
				}
				let ctx = inner
					.cache
					.restore(&layers[index].key, &rootfs)
					.map_err(fail(&layers[index].label))?;
				(ctx, index)
			},
			(None, BaseSource::Template { build_id, meta }) => {
				log.info(Some(BASE), format!("{} [{}]", layers[0].text, layers[0].key));
				let src = store.rootfs_path(build_id).map_err(fail(BASE))?;
				let lock = store.lock(build_id);
				let _guard = lock.lock().await;
				let (s, d) = (src.clone(), rootfs.clone());
				let _ = tokio::fs::remove_file(&d).await;
				// A snapshot template's rootfs is a block overlay; builds and the
				// host tools they run need a raw image.
				tokio::task::spawn_blocking(move || materialize_disk(&s, &d))
					.await
					.map_err(|e| anyhow!(e))
					.and_then(|r| r.context("cloning the base template rootfs"))
					.map_err(fail(BASE))?;
				refresh_agent = meta.agent_digest != agent_digest;
				let ctx = BuildContext {
					user:      meta.info.default_user.clone(),
					workdir:   meta.info.default_workdir.clone(),
					env:       meta.info.env.clone(),
					boot_init: meta.boot_init.clone(),
					has_bash:  false,
				};
				(ctx, 0)
			},
			(None, BaseSource::Image { image, resolved }) => {
				log.info(Some(BASE), format!("{} [{}]", layers[0].text, layers[0].key));
				let log_line = |line: String| log.info(Some(BASE), line);
				let fetched = inner
					.oci
					.fetch_layers(resolved, &log_line)
					.await
					.map_err(fail(BASE))?;
				log.info(
					Some(BASE),
					format!("Pulled {image} in {}", format_duration(base_started.elapsed())),
				);
				let unpack_started = Instant::now();
				unpack(&builder, &req, &build_dir, &rootfs, &fetched, &log)
					.await
					.map_err(fail(BASE))?;
				log.info(
					Some(BASE),
					format!(
						"Unpacked image into {} MiB rootfs in {}",
						req.disk_mb,
						format_duration(unpack_started.elapsed())
					),
				);
				needs_provision = true;
				let ctx = BuildContext {
					user:      "root".to_owned(),
					workdir:   None,
					env:       resolved.config.env_map(),
					boot_init: String::new(),
					has_bash:  false,
				};
				(ctx, 0)
			},
		};
		grow_rootfs(&rootfs, req.disk_mb)
			.await
			.map_err(fail(BASE))?;

		// Steps VM: provisioning, default user, steps.
		if needs_provision || refresh_agent || start < last {
			let steps_started = Instant::now();
			let to_run: Vec<&LayerPlan> = layers[start + 1..].iter().collect();
			let copy_hashes: BTreeSet<&str> = to_run
				.iter()
				.filter_map(|l| match &l.action {
					Action::Step(Step::Copy(_), Some(hash)) => Some(hash.as_str()),
					_ => None,
				})
				.collect();
			let context_dir = cfg
				.data_dir
				.join("cache")
				.join("context")
				.join(&req.template_id);
			for hash in &copy_hashes {
				ensure_file_context(&builder, &req, &context_dir, hash)
					.await
					.map_err(|e| {
						let label = to_run
							.iter()
							.find(|l| matches!(&l.action, Action::Step(_, Some(h)) if h == hash))
							.map_or(BASE, |l| l.label.as_str());
						BuildFailure::new(Some(label), &e)
					})?;
			}
			let shares = if copy_hashes.is_empty() {
				Vec::new()
			} else {
				vec![FsShare::ReadOnly { tag: CONTEXT_TAG.to_owned(), dir: context_dir.clone() }]
			};
			let label_on_boot = if needs_provision {
				BASE
			} else {
				to_run.first().map_or(BASE, |l| l.label.as_str())
			};
			let guard = boot_helper(&builder, &req.build_id, "steps", &rootfs, &build_dir, shares)
				.await
				.map_err(fail(label_on_boot))?;
			let vm = guard.vm().clone();
			let agent = vm.agent();
			if !copy_hashes.is_empty() {
				agent
					.mount(&MountRequest {
						source:    CONTEXT_TAG.to_owned(),
						path:      CONTEXT_MOUNT.to_owned(),
						fstype:    "virtiofs".to_owned(),
						read_only: true,
					})
					.await
					.map_err(fail(label_on_boot))?;
			}
			if refresh_agent {
				install_agent(&builder, &agent).await.map_err(fail(BASE))?;
			}
			if needs_provision {
				log.info(Some(BASE), "Provisioning sandbox template");
				let provision_started = Instant::now();
				let code = run_logged(
					&agent,
					root_script(&provision::provision_script()),
					&log,
					BASE,
					PROVISION_TIMEOUT,
				)
				.await
				.map_err(fail(BASE))?;
				if code != 0 {
					return Err(BuildFailure::new(
						Some(BASE),
						&anyhow!("provisioning failed with exit code {code}"),
					));
				}
				let distro = agent
					.fs_read(provision::DISTRO_ENV)
					.await
					.ok()
					.map(|b| String::from_utf8_lossy(&b).into_owned());
				provision::boot_init(distro.as_deref()).clone_into(&mut ctx.boot_init);
				log.info(
					Some(BASE),
					format!(
						"Provisioned in {} (init={})",
						format_duration(provision_started.elapsed()),
						ctx.boot_init
					),
				);
			}
			ctx.has_bash = agent
				.request(Op::FsStat, json!({ "path": "/bin/bash" }))
				.await
				.is_ok();
			if needs_provision && start < last {
				capture_layer(&builder, &vm, &agent, &layers[0].key, &rootfs, &ctx, CopyPolicy::Always)
					.await
					.map_err(fail(BASE))?;
			}
			for (offset, layer) in to_run.iter().enumerate() {
				let index = start + 1 + offset;
				log.info(Some(&layer.label), format!("{} [{}]", layer.text, layer.key));
				let step_started = Instant::now();
				execute(&agent, &layer.action, &mut ctx, &log, &layer.label)
					.await
					.map_err(fail(&layer.label))?;
				log.push(
					"debug",
					Some(&layer.label),
					format!("Step took {}", format_duration(step_started.elapsed())),
				);
				if index < last {
					capture_layer(
						&builder,
						&vm,
						&agent,
						&layer.key,
						&rootfs,
						&ctx,
						CopyPolicy::ReflinkOnly,
					)
					.await
					.map_err(fail(&layer.label))?;
				}
			}
			let _ = agent.fs_sync().await;
			poweroff(&agent).await;
			guard
				.wait_poweroff(POWEROFF_GRACE)
				.await
				.map_err(fail(&layers[last].label))?;
			if needs_provision || start < last {
				inner
					.cache
					.store(&layers[last].key, &rootfs, &ctx, CopyPolicy::Always)
					.await
					.map_err(fail(&layers[last].label))?;
			}
			log.info(None, format!("Steps finished in {}", format_duration(steps_started.elapsed())));
		}

		// Finalize: configure, start/ready, snapshot.
		let (start_cmd, ready_cmd) = match (&req.start_cmd, &req.ready_cmd, &base) {
			(None, None, BaseSource::Template { meta, .. }) => {
				(meta.start_cmd.clone(), meta.ready_cmd.clone())
			},
			(start, ready, _) => (start.clone(), ready.clone()),
		};
		let finalize_key = cache::layer_key(
			&layers[last].key,
			"FINALIZE",
			&[start_cmd.clone().unwrap_or_default(), ready_cmd.clone().unwrap_or_default()],
			None,
		);
		log.info(Some(FINALIZE), format!("[finalize] Finalizing template build [{finalize_key}]"));
		let (default_user, default_workdir) = match req.template_version {
			TemplateVersion::V2_1 => (ctx.user.clone(), ctx.workdir.clone()),
			TemplateVersion::V2_0 => ("user".to_owned(), None),
		};
		let finalize_started = Instant::now();
		let params = SnapshotParams {
			build_id:        &req.build_id,
			template_id:     &req.template_id,
			vcpus:           req.vcpus,
			memory_mb:       req.memory_mb,
			ctx:             &ctx,
			default_user:    &default_user,
			default_workdir: default_workdir.as_deref(),
			configure:       Some(provision::configure_script(&req.template_id, &req.build_id)),
			start_cmd:       start_cmd.as_deref(),
			ready_cmd:       ready_cmd.as_deref(),
		};
		snapshot_phase(&builder, &params, &log)
			.await
			.map_err(fail(FINALIZE))?;
		log.info(
			Some(FINALIZE),
			format!("Template snapshot captured in {}", format_duration(finalize_started.elapsed())),
		);

		let meta = ArtifactMeta {
			info: ArtifactInfo {
				build_id: req.build_id.clone(),
				node_id: cfg.node_id,
				arch: cfg.arch.clone(),
				backend: cfg.backend.clone(),
				cpu_baseline: cfg.cpu_baseline.clone(),
				has_snapshot: true,
				bytes: 0,
				envd_version: cfg.envd_version.clone(),
				vcpus: req.vcpus,
				memory_mb: req.memory_mb,
				disk_mb: disk_mb_of(&rootfs).await.unwrap_or(req.disk_mb),
				default_user,
				default_workdir,
				env: ctx.env.clone(),
			},
			template_id: req.template_id.clone(),
			boot_init: ctx.boot_init.clone(),
			start_cmd,
			ready_cmd,
			agent_digest,
			created_at_ms: now_ms(),
		};
		let info = store
			.commit(&req.build_id, meta)
			.await
			.map_err(fail(FINALIZE))?;
		log.push("debug", None, format!("Total build time {}", format_duration(started.elapsed())));
		Ok(info)
	}
}

/// Local metadata of a `fromTemplate` parent, fetching it from peers if needed.
async fn parent_meta(
	store: &ArtifactStore,
	build_id: &str,
	sources: &[String],
) -> Result<ArtifactMeta> {
	let lock = store.lock(build_id);
	let _guard = lock.lock().await;
	match store.local_meta(build_id) {
		Some(meta) => Ok(meta),
		None => store
			.fetch(build_id, sources)
			.await
			.with_context(|| format!("fetching base template build {build_id}")),
	}
}

/// Creates the sparse ext4 image and runs the unpack VM.
async fn unpack(
	builder: &Builder,
	req: &StartBuild,
	build_dir: &Path,
	rootfs: &Path,
	layers: &[oci::Layer],
	log: &BuildLog,
) -> Result<()> {
	let inner = &builder.inner;
	mkfs(rootfs, req.disk_mb).await?;
	let initramfs = builder.initramfs().await?;
	let spec = VmSpec {
		name:          vm_name(&req.build_id, "unpack"),
		boot:          Boot::Cold {
			kernel:  inner.config.kernel.clone(),
			initrd:  Some(initramfs),
			cmdline: initramfs_cmdline(&inner.config.arch),
		},
		vcpus:         HELPER_VCPUS,
		memory_mb:     HELPER_MEMORY_MB,
		disk:          Some(Disk::Direct { path: rootfs.to_owned(), read_only: false }),
		shares:        vec![FsShare::ReadOnly {
			tag: LAYERS_TAG.to_owned(),
			dir: inner.oci.blob_dir().to_owned(),
		}],
		net:           crate::vm::VmNet::None,
		snapshot_root: None,
		console_log:   build_dir.join("console-unpack.log"),
		timeout:       Some(VM_DEADLINE),
		ready_timeout: UNPACK_READY,
		extra_args:    Vec::new(),
	};
	let vm = inner.vms.launch(spec).await?;
	let guard = VmGuard { vm: Some(vm), net: None };
	let agent = guard.vm().agent();
	agent
		.mount(&MountRequest {
			source:    "/dev/vda".to_owned(),
			path:      UNPACK_ROOT.to_owned(),
			fstype:    "ext4".to_owned(),
			read_only: false,
		})
		.await
		.context("mounting the target disk")?;
	agent
		.mount(&MountRequest {
			source:    LAYERS_TAG.to_owned(),
			path:      LAYERS_MOUNT.to_owned(),
			fstype:    "virtiofs".to_owned(),
			read_only: true,
		})
		.await
		.context("mounting the layer share")?;
	let guest_layers: Vec<String> = layers
		.iter()
		.map(|l| format!("{LAYERS_MOUNT}/{}", l.digest.trim_start_matches("sha256:")))
		.collect();
	let result = agent
		.request_timeout(
			Op::OciUnpack,
			json!({ "layers": guest_layers, "dest": UNPACK_ROOT }),
			OCI_UNPACK_TIMEOUT,
		)
		.await
		.context("applying image layers")?;
	log.info(
		Some(BASE),
		format!(
			"Applied {} layers: {} files, {} whiteouts",
			layers.len(),
			result
				.get("files")
				.and_then(serde_json::Value::as_u64)
				.unwrap_or(0),
			result
				.get("whiteouts")
				.and_then(serde_json::Value::as_u64)
				.unwrap_or(0)
		),
	);
	let metadata = json!({ "ENV_ID": req.template_id, "TEMPLATE_ID": req.template_id, "BUILD_ID": req.build_id });
	agent
		.request_timeout(
			Op::Provision,
			json!({ "root": UNPACK_ROOT, "agent_dest": provision::ENVD_PATH, "metadata": metadata }),
			SCRIPT_TIMEOUT,
		)
		.await
		.context("installing the guest agent")?;
	poweroff(&agent).await;
	guard.wait_poweroff(POWEROFF_GRACE).await
}

/// Boots the rootfs with the agent as PID 1 and network (steps VM).
async fn boot_helper(
	builder: &Builder,
	build_id: &str,
	phase: &str,
	rootfs: &Path,
	build_dir: &Path,
	shares: Vec<FsShare>,
) -> Result<VmGuard> {
	let inner = &builder.inner;
	let name = vm_name(build_id, phase);
	let net = inner.vms.build_network(&name).await?;
	let spec = VmSpec {
		name,
		boot: Boot::Cold {
			kernel:  inner.config.kernel.clone(),
			initrd:  None,
			cmdline: kernel_cmdline(&inner.config.arch, provision::ENVD_PATH),
		},
		vcpus: HELPER_VCPUS,
		memory_mb: HELPER_MEMORY_MB,
		disk: Some(Disk::Direct { path: rootfs.to_owned(), read_only: false }),
		shares,
		net: net.net.clone(),
		snapshot_root: None,
		console_log: build_dir.join(format!("console-{phase}.log")),
		timeout: Some(VM_DEADLINE),
		ready_timeout: STEPS_READY,
		extra_args: Vec::new(),
	};
	let vm = inner.vms.launch(spec).await?;
	let guard = VmGuard { vm: Some(vm), net: Some(net) };
	if let Some(net) = &guard.net {
		net.configure(guard.vm())
			.await
			.context("configuring guest network")?;
	}
	sync_clock(&guard.vm().agent()).await?;
	Ok(guard)
}

/// Sets the guest wall clock from the host (TLS needs it; the guest boots at
/// the epoch).
async fn sync_clock(agent: &AgentClient) -> Result<()> {
	let unix_ns = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map_or(0, |d| u64::try_from(d.as_nanos()).unwrap_or(u64::MAX));
	agent
		.request(Op::SetTime, json!({ "unix_ns": unix_ns }))
		.await
		.context("setting the guest clock")
		.map(drop)
}

/// Replaces `/usr/bin/envd` with this node's agent (fromTemplate parents built
/// by older agents).
async fn install_agent(builder: &Builder, agent: &AgentClient) -> Result<()> {
	let binary = tokio::fs::read(&builder.inner.config.agent_binary).await?;
	let staged = format!("{}.new", provision::ENVD_PATH);
	agent.fs_write(&staged, 0o755, &binary).await?;
	let script = format!("chmod 0755 {staged} && mv -f {staged} {}", provision::ENVD_PATH);
	let (_, stderr, code) = agent.exec(&root_script(&script)).await?.output().await?;
	if code != 0 {
		bail!("installing envd failed: {}", String::from_utf8_lossy(&stderr).trim());
	}
	Ok(())
}

/// Syncs, pauses the VM, captures the rootfs into the step cache, resumes.
async fn capture_layer(
	builder: &Builder,
	vm: &Vm,
	agent: &AgentClient,
	key: &str,
	rootfs: &Path,
	ctx: &BuildContext,
	policy: CopyPolicy,
) -> Result<()> {
	agent.fs_sync().await?;
	vm.pause().await?;
	let stored = builder.inner.cache.store(key, rootfs, ctx, policy).await;
	vm.resume().await?;
	stored.map(drop)
}

/// GC4 `poweroff`; the reply may be lost when the guest powers off quickly.
async fn poweroff(agent: &AgentClient) {
	if let Err(error) = agent
		.request_timeout(Op::Poweroff, serde_json::Value::Null, Duration::from_secs(30))
		.await
	{
		tracing::debug!(%error, "poweroff request");
	}
}

/// `/bin/sh -c script` as root with the standard PATH.
fn root_script(script: &str) -> ExecRequest {
	ExecRequest {
		cmd:  vec!["/bin/sh".to_owned(), "-c".to_owned(), script.to_owned()],
		cwd:  Some("/".to_owned()),
		env:  BuildContext::default().command_env(),
		tty:  false,
		user: Some("root".to_owned()),
	}
}

/// Runs one non-base layer.
async fn execute(
	agent: &AgentClient,
	action: &Action,
	ctx: &mut BuildContext,
	log: &BuildLog,
	label: &str,
) -> Result<()> {
	match action {
		Action::Base => Ok(()),
		Action::DefaultUser => {
			run_script(agent, &steps::user_script("user", true), log, label).await?;
			"user".clone_into(&mut ctx.user);
			Ok(())
		},
		Action::Step(Step::Run { cmd, user }, _) => {
			let req = ExecRequest {
				cmd:  ctx.shell_argv(cmd),
				cwd:  None,
				env:  ctx.command_env(),
				tty:  false,
				user: Some(user.clone().unwrap_or_else(|| ctx.user.clone())),
			};
			// Upstream wraps envd errors as `failed to run command '%s': command
			// failed: %w`.
			let code = run_logged(agent, req, log, label, COMMAND_TIMEOUT)
				.await
				.map_err(|error| anyhow!("failed to run command '{cmd}': {error:#}"))?;
			if code != 0 {
				bail!("failed to run command '{cmd}': exit code {code}");
			}
			Ok(())
		},
		Action::Step(Step::Copy(spec), hash) => {
			let hash = hash
				.as_deref()
				.ok_or_else(|| anyhow!("COPY requires filesHash"))?;
			copy(agent, spec, hash, ctx, log, label).await
		},
		Action::Step(Step::Env(pairs), _) => {
			let values: Vec<&str> = pairs.iter().map(|(_, v)| v.as_str()).collect();
			let mut req = root_script(&steps::env_eval_script(&values));
			req.env = ctx.command_env();
			let (stdout, stderr, code) = agent.exec(&req).await?.output().await?;
			if code != 0 {
				bail!(
					"evaluating environment values failed: {}",
					String::from_utf8_lossy(&stderr).trim()
				);
			}
			let mut evaluated: Vec<String> = stdout
				.split(|b| *b == 0)
				.map(|v| String::from_utf8_lossy(v).into_owned())
				.collect();
			evaluated.pop();
			if evaluated.len() != pairs.len() {
				bail!(
					"evaluating environment values returned {} values for {} variables",
					evaluated.len(),
					pairs.len()
				);
			}
			for ((key, _), value) in pairs.iter().zip(evaluated) {
				ctx.env.insert(key.clone(), value);
			}
			Ok(())
		},
		Action::Step(Step::Workdir(path), _) => {
			let dir = steps::resolve_workdir(ctx.workdir.as_deref(), path);
			run_script(agent, &steps::workdir_script(&dir, &ctx.user), log, label).await?;
			ctx.workdir = Some(dir);
			Ok(())
		},
		Action::Step(Step::User { name, sudo }, _) => {
			run_script(agent, &steps::user_script(name, *sudo), log, label).await?;
			ctx.user.clone_from(name);
			Ok(())
		},
	}
}

async fn copy(
	agent: &AgentClient,
	spec: &CopySpec,
	hash: &str,
	ctx: &BuildContext,
	log: &BuildLog,
	label: &str,
) -> Result<()> {
	run_script(agent, &spec.script(hash, ctx), log, label).await
}

async fn run_script(agent: &AgentClient, script: &str, log: &BuildLog, label: &str) -> Result<()> {
	let code = run_logged(agent, root_script(script), log, label, SCRIPT_TIMEOUT).await?;
	if code != 0 {
		bail!("step failed with exit code {code}");
	}
	Ok(())
}

/// Runs a command, streaming its output into the build log; returns the
/// exit code (`128 + signal` when killed).
async fn run_logged(
	agent: &AgentClient,
	req: ExecRequest,
	log: &BuildLog,
	label: &str,
	timeout: Duration,
) -> Result<i32> {
	let mut session = agent.exec(&req).await?;
	let deadline = tokio::time::Instant::now() + timeout;
	let (mut out, mut err) = (LineSplitter::default(), LineSplitter::default());
	loop {
		let event = match tokio::time::timeout_at(deadline, session.next()).await {
			Ok(Some(event)) => event,
			Ok(None) => bail!("command session ended without an exit status"),
			Err(_) => {
				let _ = session.kill(libc::SIGKILL).await;
				bail!("command timed out after {}", format_duration(timeout));
			},
		};
		match event {
			ExecEvent::Stdout(chunk) => out
				.feed(&chunk)
				.into_iter()
				.for_each(|line| log.info(Some(label), line)),
			ExecEvent::Stderr(chunk) => err
				.feed(&chunk)
				.into_iter()
				.for_each(|line| log.info(Some(label), line)),
			ExecEvent::Exit { code, signal } => {
				out.finish()
					.into_iter()
					.chain(err.finish())
					.for_each(|line| log.info(Some(label), line));
				return Ok(signal.map_or(code, |s| 128 + s));
			},
			ExecEvent::Failed(error) => bail!("command failed: {error}"),
		}
	}
}

/// Fetches and extracts a COPY file context to `<context_dir>/<hash>/`.
async fn ensure_file_context(
	builder: &Builder,
	req: &StartBuild,
	context_dir: &Path,
	hash: &str,
) -> Result<()> {
	let dest = context_dir.join(hash);
	if tokio::fs::try_exists(&dest).await.unwrap_or(false) {
		return Ok(());
	}
	tokio::fs::create_dir_all(context_dir).await?;
	let cfg = &builder.inner.config;
	let archive = context_dir.join(format!(".{hash}.{:08x}.tar", rand::random::<u32>()));
	let mut errors = Vec::new();
	let mut fetched = false;
	for source in &req.file_sources {
		let attempt = async {
			let client = NodeClient::new(source, &cfg.secret, cfg.http.clone())?;
			let response = client.get_file(&req.template_id, hash).await?;
			let mut file = tokio::fs::File::create(&archive).await?;
			let mut stream = response.bytes_stream().map_err(std::io::Error::other);
			while let Some(chunk) = stream.try_next().await? {
				file.write_all(&chunk).await?;
			}
			file.sync_all().await?;
			Ok::<_, anyhow::Error>(())
		}
		.await;
		match attempt {
			Ok(()) => {
				fetched = true;
				break;
			},
			Err(error) => errors.push(format!("{source}: {error:#}")),
		}
	}
	if !fetched {
		let _ = tokio::fs::remove_file(&archive).await;
		if errors.is_empty() {
			bail!("file context {hash} is not available (no file sources)");
		}
		bail!("fetching file context {hash} failed: {}", errors.join("; "));
	}
	let staging = context_dir.join(format!(".{hash}.{:08x}.d", rand::random::<u32>()));
	let (a, s) = (archive.clone(), staging.clone());
	let extracted = tokio::task::spawn_blocking(move || extract_context(&a, &s))
		.await
		.map_err(|e| anyhow!(e))
		.and_then(|r| r);
	let _ = tokio::fs::remove_file(&archive).await;
	if let Err(error) = extracted {
		let _ = tokio::fs::remove_dir_all(&staging).await;
		return Err(error.context(format!("extracting file context {hash}")));
	}
	if let Err(error) = tokio::fs::rename(&staging, &dest).await {
		let _ = tokio::fs::remove_dir_all(&staging).await;
		if !tokio::fs::try_exists(&dest).await.unwrap_or(false) {
			return Err(error.into());
		}
	}
	Ok(())
}

/// Extracts a (gzip-sniffed) tar archive keeping permission bits (incl.
/// setuid) but not ownership; entries escaping the destination are skipped.
pub(super) fn extract_context(archive: &Path, dest: &Path) -> Result<()> {
	use std::io::{Read, Seek};
	let mut file = std::fs::File::open(archive)?;
	let mut magic = [0u8; 2];
	let gzip = file.read(&mut magic)? == 2 && magic == [0x1f, 0x8b];
	file.rewind()?;
	let reader: Box<dyn Read> = if gzip {
		Box::new(flate2::read::GzDecoder::new(file))
	} else {
		Box::new(file)
	};
	let mut tar = tar::Archive::new(reader);
	tar.set_preserve_permissions(true);
	tar.set_preserve_ownerships(false);
	tar.set_unpack_xattrs(false);
	tar.set_overwrite(true);
	std::fs::create_dir_all(dest)?;
	tar.unpack(dest)?;
	Ok(())
}

/// Inputs of the template-snapshot phase (finalize and regeneration).
pub(super) struct SnapshotParams<'a> {
	pub build_id:        &'a str,
	pub template_id:     &'a str,
	pub vcpus:           u32,
	pub memory_mb:       u64,
	/// Execution context of start/ready commands and the boot `init=`.
	pub ctx:             &'a BuildContext,
	pub default_user:    &'a str,
	pub default_workdir: Option<&'a str>,
	/// `configure.sh` (finalize only).
	pub configure:       Option<String>,
	pub start_cmd:       Option<&'a str>,
	pub ready_cmd:       Option<&'a str>,
}

/// Boots the rootfs with its own init, runs configure + start/ready, and
/// captures `builds/<id>/snapshot`.
pub(super) async fn snapshot_phase(
	builder: &Builder,
	p: &SnapshotParams<'_>,
	log: &BuildLog,
) -> Result<()> {
	let inner = &builder.inner;
	let store = &inner.store;
	let build_dir = store.build_dir(p.build_id)?;
	let snapshot_dir = build_dir.join(SNAPSHOT);
	if tokio::fs::try_exists(&snapshot_dir).await.unwrap_or(false) {
		tokio::fs::remove_dir_all(&snapshot_dir).await?;
	}
	let name = vm_name(p.build_id, "template");
	let net = inner.vms.build_network(&name).await?;
	let spec = VmSpec {
		name,
		boot: Boot::Cold {
			kernel:  inner.config.kernel.clone(),
			initrd:  None,
			cmdline: kernel_cmdline(&inner.config.arch, &p.ctx.boot_init),
		},
		vcpus: p.vcpus,
		memory_mb: p.memory_mb,
		disk: Some(Disk::Direct { path: build_dir.join(ROOTFS), read_only: false }),
		shares: Vec::new(),
		net: net.net.clone(),
		snapshot_root: Some(build_dir.clone()),
		console_log: build_dir.join("console-template.log"),
		timeout: Some(VM_DEADLINE),
		ready_timeout: TEMPLATE_READY,
		extra_args: Vec::new(),
	};
	let boot_started = Instant::now();
	let vm = inner
		.vms
		.launch(spec)
		.await
		.context("booting the template VM")?;
	let guard = VmGuard { vm: Some(vm.clone()), net: Some(net) };
	if let Some(net) = &guard.net {
		net.configure(&vm)
			.await
			.context("configuring guest network")?;
	}
	sync_clock(&vm.agent()).await?;
	log.push(
		"debug",
		Some(FINALIZE),
		format!("Template VM booted in {}", format_duration(boot_started.elapsed())),
	);
	let agent = vm.agent();
	let settle_started = Instant::now();
	let (..) = agent
		.exec(&root_script(SETTLE_INIT))
		.await?
		.output()
		.await?;
	log.push(
		"debug",
		Some(FINALIZE),
		format!("Init system settled in {}", format_duration(settle_started.elapsed())),
	);
	if let Some(script) = &p.configure {
		let code = run_logged(&agent, root_script(script), log, FINALIZE, CONFIGURE_TIMEOUT).await?;
		if code != 0 {
			bail!("configuration script failed with exit code {code}");
		}
	}
	let init = GuestInit {
		env_vars:          p.ctx.env.clone(),
		envd_access_token: String::new(),
		default_user:      Some(p.default_user.to_owned()),
		default_workdir:   p.default_workdir.map(str::to_owned),
		sandbox_id:        String::new(),
		template_id:       p.template_id.to_owned(),
		ca_bundle:         None,
	};
	EnvdClient::new(vm.clone())
		.init(&init)
		.await
		.context("envd did not accept /init")?;

	let run_as = |cmd: &str| ExecRequest {
		cmd:  p.ctx.shell_argv(cmd),
		cwd:  None,
		env:  p.ctx.command_env(),
		tty:  false,
		user: Some(p.ctx.user.clone()),
	};
	let (exit_tx, mut exit_rx) = tokio::sync::watch::channel(None::<i32>);
	let start_task = match p.start_cmd.filter(|c| !c.trim().is_empty()) {
		Some(cmd) => {
			log.info(Some(FINALIZE), format!("Running start command: {cmd}"));
			let mut session = agent.exec(&run_as(cmd)).await?;
			let log = log.clone();
			Some(tokio::spawn(async move {
				let (mut out, mut err) = (LineSplitter::default(), LineSplitter::default());
				while let Some(event) = session.next().await {
					match event {
						ExecEvent::Stdout(chunk) => out
							.feed(&chunk)
							.into_iter()
							.for_each(|l| log.info(Some(FINALIZE), l)),
						ExecEvent::Stderr(chunk) => err
							.feed(&chunk)
							.into_iter()
							.for_each(|l| log.info(Some(FINALIZE), l)),
						ExecEvent::Exit { code, signal } => {
							let _ = exit_tx.send(Some(signal.map_or(code, |s| 128 + s)));
							return;
						},
						ExecEvent::Failed(error) => {
							log.error(Some(FINALIZE), format!("start command: {error}"));
							let _ = exit_tx.send(Some(-1));
							return;
						},
					}
				}
			}))
		},
		None => None,
	};
	let ready = match (p.ready_cmd.filter(|c| !c.trim().is_empty()), start_task.is_some()) {
		(Some(cmd), _) => cmd,
		(None, true) => "sleep 20",
		(None, false) => "sleep 0",
	};
	log.info(Some(FINALIZE), format!("Waiting for template to be ready: {ready}"));
	let ready_started = Instant::now();
	let result = async {
		loop {
			if let Some(code) = *exit_rx.borrow_and_update()
				&& code != 0
			{
				bail!("start command failed with exit code {code}");
			}
			let remaining = READY_TIMEOUT.saturating_sub(ready_started.elapsed());
			if remaining.is_zero() {
				bail!("ready command timed out after {}", format_duration(READY_TIMEOUT));
			}
			let (_, _, code) = match tokio::time::timeout(remaining, async {
				agent.exec(&run_as(ready)).await?.output().await
			})
			.await
			{
				Ok(result) => result?,
				Err(_) => bail!("ready command timed out after {}", format_duration(READY_TIMEOUT)),
			};
			if code == 0 {
				return Ok(());
			}
			tokio::time::sleep(
				READY_INTERVAL.min(READY_TIMEOUT.saturating_sub(ready_started.elapsed())),
			)
			.await;
		}
	}
	.await;
	if let Some(task) = &start_task {
		task.abort();
	}
	result?;
	log.info(Some(FINALIZE), "Template is ready");
	agent.fs_sync().await?;
	vm.pause().await?;
	vm.snapshot(SNAPSHOT)
		.await
		.context("capturing the template snapshot")?;
	guard.kill().await;
	if !tokio::fs::try_exists(snapshot_dir.join("current-generation"))
		.await
		.unwrap_or(false)
	{
		bail!("the VMM reported a snapshot but {} is incomplete", snapshot_dir.display());
	}
	Ok(())
}

/// Recreates the template snapshot of a local artifact on this host
/// (incompatible or missing snapshot), then re-commits its metadata.
pub(super) async fn regenerate_snapshot(
	builder: &Builder,
	mut meta: ArtifactMeta,
	log: &BuildLog,
) -> Result<ArtifactInfo> {
	let host = builder.inner.store.host().clone();
	log.info(
		Some(FINALIZE),
		format!(
			"Regenerating the template snapshot of {} for {}/{}",
			meta.info.build_id, host.arch, host.backend
		),
	);
	let ctx = BuildContext {
		user:      meta.info.default_user.clone(),
		workdir:   meta.info.default_workdir.clone(),
		env:       meta.info.env.clone(),
		boot_init: meta.boot_init.clone(),
		has_bash:  true,
	};
	let started = Instant::now();
	let params = SnapshotParams {
		build_id:        &meta.info.build_id,
		template_id:     &meta.template_id,
		vcpus:           meta.info.vcpus,
		memory_mb:       meta.info.memory_mb,
		ctx:             &ctx,
		default_user:    &meta.info.default_user,
		default_workdir: meta.info.default_workdir.as_deref(),
		configure:       None,
		start_cmd:       meta.start_cmd.as_deref(),
		ready_cmd:       meta.ready_cmd.as_deref(),
	};
	snapshot_phase(builder, &params, log).await?;
	log.info(
		Some(FINALIZE),
		format!("Snapshot regenerated in {}", format_duration(started.elapsed())),
	);
	meta.info.node_id = host.node_id;
	meta.info.arch = host.arch;
	meta.info.backend = host.backend;
	meta.info.cpu_baseline = host.cpu_baseline;
	meta.info.has_snapshot = true;
	let build_id = meta.info.build_id.clone();
	builder.inner.store.commit(&build_id, meta).await
}

/// Removes partial outputs of a failed or cancelled build (logs are kept).
pub(super) fn discard_outputs(store: &ArtifactStore, build_id: &str) {
	let Ok(dir) = store.build_dir(build_id) else {
		return;
	};
	for file in [ROOTFS, META, MANIFEST] {
		let _ = std::fs::remove_file(dir.join(file));
	}
	let _ = std::fs::remove_dir_all(dir.join(SNAPSHOT));
}

/// VM instance name for a build phase (`b-<16 hex>-<phase>`).
fn vm_name(build_id: &str, phase: &str) -> String {
	let id: String = build_id
		.chars()
		.filter(char::is_ascii_alphanumeric)
		.map(|c| c.to_ascii_lowercase())
		.take(16)
		.collect();
	format!("b-{id}-{phase}")
}

/// Kernel command line booting the generated initramfs (agent as `/init`).
fn initramfs_cmdline(arch: &str) -> String {
	let earlycon = if arch == "aarch64" {
		" earlycon=uart8250,mmio,0x9000000"
	} else {
		""
	};
	format!("console=ttyS0 reboot=t panic=-1{earlycon} rdinit=/init")
}

/// Upstream-style duration (`1m2.5s`, `850ms`).
pub(super) fn format_duration(d: Duration) -> String {
	let ms = d.as_millis();
	if ms < 1000 {
		return format!("{ms}ms");
	}
	let secs = d.as_secs_f64();
	if secs < 60.0 {
		return format!("{secs:.1}s");
	}
	let minutes = d.as_secs() / 60;
	let rest = secs - (minutes * 60) as f64;
	format!("{minutes}m{rest:.1}s")
}

/// Locates an e2fsprogs tool (Homebrew keg on macOS, sbin dirs, then PATH).
fn e2fs_tool(name: &str) -> PathBuf {
	const DIRS: &[&str] = &[
		"/opt/homebrew/opt/e2fsprogs/sbin",
		"/usr/local/opt/e2fsprogs/sbin",
		"/usr/sbin",
		"/sbin",
		"/usr/bin",
		"/bin",
	];
	DIRS
		.iter()
		.map(|dir| Path::new(dir).join(name))
		.find(|p| p.is_file())
		.unwrap_or_else(|| PathBuf::from(name))
}

async fn run_tool(tool: &str, args: &[&std::ffi::OsStr], ok_codes: &[i32]) -> Result<()> {
	let path = e2fs_tool(tool);
	let output = tokio::process::Command::new(&path)
		.args(args)
		.output()
		.await
		.with_context(|| format!("running {} (install e2fsprogs)", path.display()))?;
	let code = output.status.code().unwrap_or(-1);
	if !ok_codes.contains(&code) {
		bail!("{tool} failed ({code}): {}", String::from_utf8_lossy(&output.stderr).trim());
	}
	Ok(())
}

/// Creates a sparse ext4 image of `disk_mb` MiB.
async fn mkfs(path: &Path, disk_mb: u64) -> Result<()> {
	let _ = tokio::fs::remove_file(path).await;
	let file = tokio::fs::File::create(path).await?;
	file.set_len(disk_mb << 20).await?;
	drop(file);
	run_tool(
		"mkfs.ext4",
		&[
			"-q".as_ref(),
			"-F".as_ref(),
			"-m".as_ref(),
			"0".as_ref(),
			"-L".as_ref(),
			"rootfs".as_ref(),
			path.as_os_str(),
		],
		&[0],
	)
	.await
}

/// Grows (never shrinks) an ext4 image to `disk_mb` MiB.
async fn grow_rootfs(path: &Path, disk_mb: u64) -> Result<()> {
	let target = disk_mb << 20;
	let len = tokio::fs::metadata(path).await?.len();
	if len >= target {
		return Ok(());
	}
	// Replays the journal of images captured while mounted; 1 = errors fixed.
	run_tool("e2fsck", &["-f".as_ref(), "-y".as_ref(), path.as_os_str()], &[0, 1]).await?;
	tokio::fs::OpenOptions::new()
		.write(true)
		.open(path)
		.await?
		.set_len(target)
		.await?;
	run_tool("resize2fs", &[path.as_os_str()], &[0]).await
}

async fn disk_mb_of(path: &Path) -> Result<u64> {
	Ok(tokio::fs::metadata(path).await?.len() >> 20)
}

#[cfg(test)]
mod tests {
	use std::io::Write;

	use super::*;

	/// GC4 agent stub: answers `hello`; `exec` fails for user `root123` the way
	/// the real agent does and exits with code 7 otherwise.
	async fn stub_agent(stream: tokio::io::DuplexStream) {
		use tokio::io::{AsyncReadExt, AsyncWriteExt};
		use vmon_agent::proto;
		let (mut reader, mut writer) = tokio::io::split(stream);
		let mut header = [0u8; 9];
		while reader.read_exact(&mut header).await.is_ok() {
			if header == proto::SYNC {
				continue;
			}
			let len = u32::from_le_bytes(header[..4].try_into().unwrap()) as usize;
			let id = u32::from_le_bytes(header[5..9].try_into().unwrap());
			let mut payload = vec![0u8; len];
			reader.read_exact(&mut payload).await.unwrap();
			if header[4] != proto::FRAME_REQ {
				continue;
			}
			let req: serde_json::Value = serde_json::from_slice(&payload).unwrap();
			let (ty, body) = match serde_json::from_value::<Op>(req["op"].clone()).unwrap() {
				Op::Hello => (proto::FRAME_RESP, json!({ "ok": true, "epoch": req["epoch"] })),
				Op::Exec if req["user"] == "root123" => (
					proto::FRAME_RESP,
					json!({ "ok": false, "error": "unauthenticated: invalid username: 'root123'" }),
				),
				Op::Exec => (proto::FRAME_EXIT, json!({ "code": 7, "signal": null })),
				op => panic!("unexpected op {op:?}"),
			};
			let body = body.to_string();
			let mut frame = Vec::with_capacity(9 + body.len());
			frame.extend_from_slice(&u32::try_from(body.len()).unwrap().to_le_bytes());
			frame.push(ty);
			frame.extend_from_slice(&id.to_le_bytes());
			frame.extend_from_slice(body.as_bytes());
			writer.write_all(&frame).await.unwrap();
		}
	}

	#[tokio::test]
	async fn run_step_failures_use_upstream_wording() {
		let (host, guest) = tokio::io::duplex(1 << 16);
		tokio::spawn(stub_agent(guest));
		let agent = AgentClient::handshake(host).await.unwrap();
		let dir = tempfile::tempdir().unwrap();
		let log = BuildLog::create(&dir.path().join("build.log"));
		let mut ctx = BuildContext { user: "user".into(), ..BuildContext::default() };
		let run = |user: Option<&str>| {
			Action::Step(Step::Run { cmd: "whoami".into(), user: user.map(str::to_owned) }, None)
		};

		let error = execute(&agent, &run(Some("root123")), &mut ctx, &log, "1")
			.await
			.unwrap_err();
		assert_eq!(
			format!("{error:#}"),
			"failed to run command 'whoami': command failed: unauthenticated: invalid username: \
			 'root123'",
			"E2B SDK test_run_command_as_user_that_does_not_exist matches this text"
		);
		let error = execute(&agent, &run(None), &mut ctx, &log, "1")
			.await
			.unwrap_err();
		assert_eq!(format!("{error:#}"), "failed to run command 'whoami': exit code 7");
	}
	#[test]
	fn vm_names_fit_runtime_constraints() {
		let name = vm_name("0F1E2D3C-4B5A-6978-8796-A5B4C3D2E1F0", "template");
		assert_eq!(name, "b-0f1e2d3c4b5a6978-template");
		assert!(name.len() <= 40);
	}

	#[test]
	fn durations_render_like_upstream() {
		assert_eq!(format_duration(Duration::from_millis(850)), "850ms");
		assert_eq!(format_duration(Duration::from_millis(12_340)), "12.3s");
		assert_eq!(format_duration(Duration::from_millis(62_500)), "1m2.5s");
	}

	fn tar_with(gzip: bool) -> Vec<u8> {
		let mut builder = tar::Builder::new(Vec::new());
		let mut header = tar::Header::new_gnu();
		header.set_size(3);
		header.set_mode(0o4755);
		header.set_uid(4242);
		header.set_cksum();
		builder
			.append_data(&mut header, "bin/tool", &b"abc"[..])
			.unwrap();
		let mut header = tar::Header::new_gnu();
		header.set_size(1);
		header.set_mode(0o644);
		header.set_cksum();
		builder
			.append_data(&mut header, "../escape", &b"x"[..])
			.unwrap_or(());
		let data = builder.into_inner().unwrap();
		if !gzip {
			return data;
		}
		let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
		encoder.write_all(&data).unwrap();
		encoder.finish().unwrap()
	}

	#[test]
	fn context_extraction_sniffs_gzip_keeps_setuid_and_stays_inside() {
		use std::os::unix::fs::PermissionsExt;
		for gzip in [true, false] {
			let dir = tempfile::tempdir().unwrap();
			let archive = dir.path().join("ctx.tar");
			std::fs::write(&archive, tar_with(gzip)).unwrap();
			let dest = dir.path().join("out");
			extract_context(&archive, &dest).unwrap();
			let tool = dest.join("bin/tool");
			assert_eq!(std::fs::read(&tool).unwrap(), b"abc");
			assert_eq!(
				std::fs::metadata(&tool).unwrap().permissions().mode() & 0o7777,
				0o4755,
				"gzip={gzip}"
			);
			assert!(!dir.path().join("escape").exists());
		}
	}
}
