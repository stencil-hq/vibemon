//! Real-VM node lifecycle test (gated: `VMON_E2E=1`).
//!
//! Prepares a template build by hand (cold boot of a prepared ext4 rootfs
//! with the guest agent at `/.vmon/agent`, then a memory snapshot), serves
//! the node API over HTTP, and drives one sandbox through create (fork) →
//! tunnel to envd `/health` → pause → resume → kill, printing latencies.
//!
//! Environment: `VMON_BIN` (signed `vmon` with the `vmm` subcommand),
//! `VMON_KERNEL` (guest kernel), `VMON_TEST_ROOTFS` (ext4 with the agent),
//! `VMON_AGENT` (static guest agent, for the builder config).

use std::{
	collections::BTreeMap,
	path::PathBuf,
	sync::Arc,
	time::{Duration, Instant},
};

use bytes::Bytes;
use http_body_util::{BodyExt, Empty};
use hyper_util::rt::TokioIo;
use vmon_cluster::NodeId;
use vmon_node::{
	api::{
		ArtifactInfo, CreateSandbox, GuestInit, LogQuery, NetworkPolicy, PauseSandbox, ResumeSandbox,
		SandboxState, SnapshotSandbox, VolumeMount,
	},
	artifacts::{self, ArtifactMeta, copy::clone_file},
	build::{BuildConfig, Builder},
	client::NodeClient,
	egress::{Egress, EgressConfig},
	envd::EnvdClient,
	host,
	net::{NetManager, NetMode},
	sandbox::{ManagerConfig, SandboxManager},
	server,
	vm::{Boot, Disk, VmRuntime, VmRuntimeConfig, VmSpec, VmmIsolation},
	volumes::VolumeStore,
};

const SECRET: &str = "node-e2e-secret";

fn env_path(name: &str) -> PathBuf {
	PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set")))
}

/// GET /health through a node tunnel; returns the status code.
/// SIGKILLs every sandbox VMM still running when the test ends (VMMs run
/// in their own process group and would otherwise outlive a failed test).
struct KillOnDrop(Arc<SandboxManager>);

impl Drop for KillOnDrop {
	fn drop(&mut self) {
		for sandbox in self.0.list() {
			if let Ok(vm) = self.0.live_vm(&sandbox.sandbox_id)
				&& let Ok(pid) = libc::pid_t::try_from(vm.pid())
			{
				// SAFETY: kill(2) on a VMM this test launched.
				unsafe { libc::kill(pid, libc::SIGKILL) };
			}
		}
	}
}

async fn health_via_tunnel(client: &NodeClient, id: &str) -> u16 {
	let io = client.open_tunnel(id, 49983).await.expect("open tunnel");
	let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(io))
		.await
		.expect("http handshake");
	tokio::spawn(connection);
	let request = http::Request::get("/health")
		.header("host", "localhost")
		.body(Empty::<Bytes>::new())
		.unwrap();
	let response = sender.send_request(request).await.expect("health request");
	let status = response.status().as_u16();
	let _ = response.into_body().collect().await;
	status
}

/// Template build, VM runtime, and node API served over HTTP.
struct Harness {
	data:     tempfile::TempDir,
	run_dir:  PathBuf,
	build_id: String,
	manager:  Arc<SandboxManager>,
	client:   NodeClient,
	_reaper:  KillOnDrop,
}

impl Drop for Harness {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.run_dir);
	}
}

/// vCPUs of the hand-built template (`VMON_E2E_VCPUS`, default 2: SMP
/// guests exercise cross-vCPU restore state).
fn template_vcpus() -> u32 {
	std::env::var("VMON_E2E_VCPUS")
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(2)
}

fn e2e_enabled() -> bool {
	let enabled = std::env::var("VMON_E2E").as_deref() == Ok("1");
	if !enabled {
		eprintln!("skipping: set VMON_E2E=1");
	}
	enabled
}

/// Builds the template by hand and serves the sandbox routes.
async fn harness(boot_concurrency: usize, template_load: Option<&str>) -> Harness {
	let vmon_bin = env_path("VMON_BIN");
	let kernel = env_path("VMON_KERNEL");
	let rootfs = env_path("VMON_TEST_ROOTFS");
	let agent = env_path("VMON_AGENT");
	let data = tempfile::Builder::new()
		.prefix("vmon-node-e2e")
		.tempdir_in("/tmp")
		.unwrap();
	let run_dir = PathBuf::from(format!("/tmp/vmon-e2e-{}", std::process::id()));
	let arch = vmon_node::assets::host_arch().to_owned();
	let cpu_baseline = host::cpu_baseline(&vmon_bin).await.expect("cpu baseline");

	let net = NetManager::new(NetMode::Auto, "172.20.0.0/16", None).expect("net manager");
	let vms = VmRuntime::new(VmRuntimeConfig {
		vmon_bin: vmon_bin.clone(),
		run_dir: run_dir.clone(),
		boot_concurrency,
		// Root (Linux TAP runs) launches VMMs without the uid-drop sandbox.
		// SAFETY: geteuid has no preconditions.
		isolation: if unsafe { libc::geteuid() } == 0 {
			VmmIsolation::Disabled
		} else {
			VmmIsolation::Default
		},
		net,
	})
	.unwrap();
	let egress = Egress::start(EgressConfig {
		data_dir:       data.path().to_owned(),
		internal_allow: Vec::new(),
		proxy_port:     0,
	})
	.await
	.unwrap();
	let builder = Builder::new(
		BuildConfig {
			data_dir:        data.path().to_owned(),
			node_id:         NodeId(7),
			arch:            arch.clone(),
			backend:         host::backend().into(),
			cpu_baseline:    cpu_baseline.clone(),
			envd_version:    vmon_agent::envd::ENVD_VERSION.into(),
			kernel:          kernel.clone(),
			agent_binary:    agent,
			max_parallel:    1,
			step_cache_mb:   u64::MAX,
			secret:          SECRET.into(),
			http:            reqwest::Client::new(),
			registry_mirror: None,
			object_store:    None,
		},
		vms.clone(),
	)
	.unwrap();

	// --- Template build by hand: cold boot, envd healthy, snapshot. ---
	let build_id = uuid::Uuid::new_v4().to_string();
	let store = builder.artifacts();
	let build_dir = store.build_dir(&build_id).unwrap();
	std::fs::create_dir_all(&build_dir).unwrap();
	if let Ok(existing) = std::env::var("VMON_E2E_BUILD_DIR") {
		// Reuse a real template build (e.g. `base` from a running stack).
		let existing = PathBuf::from(existing);
		clone_file(&existing.join("rootfs.ext4"), &store.rootfs_path(&build_id).unwrap()).unwrap();
		let snapshot = store.snapshot_dir(&build_id).unwrap();
		std::fs::create_dir_all(&snapshot).unwrap();
		for entry in std::fs::read_dir(existing.join("snapshot")).unwrap() {
			let entry = entry.unwrap();
			clone_file(&entry.path(), &snapshot.join(entry.file_name())).unwrap();
		}
		for file in ["meta.json", "manifest.json"] {
			std::fs::copy(existing.join(file), build_dir.join(file)).unwrap();
		}
		assert!(store.local(&build_id).is_some(), "copied build is complete");
	} else {
		let template_rootfs = store.rootfs_path(&build_id).unwrap();
		let clone = Instant::now();
		clone_file(&rootfs, &template_rootfs).unwrap();
		eprintln!(
			"clone of the {} MiB template rootfs: {:?}",
			std::fs::metadata(&rootfs).unwrap().len() >> 20,
			clone.elapsed()
		);
		let build_net = vms.build_network("tpl-e2e").await.unwrap();
		let started = Instant::now();
		let vm = vms
			.launch(VmSpec {
				name:          "tpl-e2e".into(),
				boot:          Boot::Cold {
					kernel,
					initrd: None,
					cmdline: artifacts::kernel_cmdline(&arch, "/.vmon/agent"),
				},
				vcpus:         template_vcpus(),
				memory_mb:     256,
				disk:          Some(Disk::Direct { path: template_rootfs, read_only: false }),
				shares:        Vec::new(),
				net:           build_net.net.clone(),
				snapshot_root: Some(build_dir.clone()),
				console_log:   build_dir.join("build.log"),
				timeout:       Some(Duration::from_mins(10)),
				ready_timeout: Duration::from_mins(1),
				extra_args:    Vec::new(),
			})
			.await
			.expect("template boot");
		build_net.configure(&vm).await.expect("template net_config");
		EnvdClient::new(vm.clone())
			.health()
			.await
			.expect("template envd health");
		eprintln!("template cold boot to envd healthy: {:?}", started.elapsed());
		vm.agent().fs_sync().await.unwrap();
		if let Some(load) = template_load {
			// Guest I/O while the snapshot is taken, so vCPUs are often inside
			// virtio interrupt handlers when the pause lands.
			let req = vmon_node::gc4::ExecRequest {
				cmd: vec!["/bin/sh".into(), "-c".into(), format!("({load}) >/dev/null 2>&1 &")],
				..Default::default()
			};
			vm.agent().exec(&req).await.unwrap().output().await.unwrap();
			tokio::time::sleep(Duration::from_millis(300)).await;
		}
		vm.pause().await.unwrap();
		let snap = Instant::now();
		vm.snapshot("snapshot").await.unwrap();
		eprintln!("template snapshot (256 MiB): {:?}", snap.elapsed());
		vm.shutdown(Duration::from_secs(5)).await;
		drop(build_net);
		let info = ArtifactInfo {
			build_id: build_id.clone(),
			node_id: NodeId(7),
			arch: arch.clone(),
			backend: host::backend().into(),
			cpu_baseline,
			has_snapshot: true,
			bytes: 0,
			envd_version: vmon_agent::envd::ENVD_VERSION.into(),
			vcpus: template_vcpus(),
			memory_mb: 256,
			disk_mb: 512,
			default_user: "root".into(),
			default_workdir: None,
			env: BTreeMap::new(),
		};
		store
			.commit(&build_id, ArtifactMeta {
				info,
				template_id: "tple2e".into(),
				boot_init: "/.vmon/agent".into(),
				start_cmd: None,
				ready_cmd: None,
				agent_digest: String::new(),
				created_at_ms: 0,
			})
			.await
			.expect("commit artifact");
	}

	// --- Node API over HTTP. ---
	let volumes = VolumeStore::new(data.path().join("volumes")).unwrap();
	let manager = SandboxManager::new(
		ManagerConfig {
			data_dir: data.path().to_owned(),
			arch,
			kernel: env_path("VMON_KERNEL"),
			envd_version: vmon_agent::envd::ENVD_VERSION.into(),
			run_dir: run_dir.clone(),
			object_store: None,
			lazy_resume: false,
			s3_mounts_allow_private: false,
		},
		vms,
		builder,
		egress,
		volumes,
		None,
	)
	.unwrap();
	let reaper = KillOnDrop(Arc::clone(&manager));
	let router = axum::Router::new()
		.nest(
			vmon_node::api::PREFIX,
			server::sandbox_routes(Arc::clone(&manager), server::Peers::default()),
		)
		.layer(vmon_cluster::require_secret(Arc::from(SECRET.as_bytes())));
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let url = format!("http://{}", listener.local_addr().unwrap());
	tokio::spawn(async move { axum::serve(listener, router).await });
	let client = NodeClient::new(&url, SECRET, reqwest::Client::new()).unwrap();

	Harness { data, run_dir, build_id, manager, client, _reaper: reaper }
}

#[tokio::test(flavor = "multi_thread")]
async fn sandbox_fork_tunnel_pause_resume_kill() {
	if !e2e_enabled() {
		return;
	}
	let harness = harness(4, None).await;
	let (client, manager, build_id, data) =
		(&harness.client, &harness.manager, harness.build_id.clone(), &harness.data);
	let id = "ie2etestsandbox000001".to_owned();
	let now = vmon_node::sandbox::now_ms();
	let init_template = GuestInit {
		env_vars:          BTreeMap::from([("HELLO".into(), "world".into())]),
		envd_access_token: String::new(),
		default_user:      Some("root".into()),
		default_workdir:   None,
		sandbox_id:        id.clone(),
		template_id:       "tple2e".into(),
		ca_bundle:         None,
	};
	let init = init_template.clone();
	let create = Instant::now();
	let created = client
		.create_sandbox(&CreateSandbox {
			sandbox_id:       id.clone(),
			team_id:          "team".into(),
			template_id:      "tple2e".into(),
			build_id:         build_id.clone(),
			artifact_sources: Vec::new(),
			vcpus:            1,
			memory_mb:        256,
			disk_mb:          512,
			end_at_ms:        now + 300_000,
			init:             init.clone(),
			network:          NetworkPolicy::default(),
			volume_mounts:    Vec::new(),
			s3_mounts:        Vec::new(),
			cold_boot:        false,
		})
		.await
		.expect("create sandbox");
	let create_latency = create.elapsed();
	assert_eq!(created.state, SandboxState::Running);
	eprintln!("create (fork + net_config + envd /init): {create_latency:?}");

	let vmm_metrics = manager
		.live_vm(&id)
		.unwrap()
		.control()
		.metrics()
		.await
		.expect("vmm metrics");
	eprintln!(
		"vmm fork_duration_ms={} (in-VMM restore of the template snapshot incl. disk overlay)",
		vmm_metrics["fork_duration_ms"]
	);
	let tunnel = Instant::now();
	assert_eq!(health_via_tunnel(client, &id).await, 204);
	eprintln!("first tunnel + GET /health: {:?}", tunnel.elapsed());
	let closed = client
		.open_tunnel(&id, 9)
		.await
		.expect_err("port 9 is closed");
	assert_eq!(closed.code(), "port_closed");
	assert!(
		client
			.open_tunnel("inotthere00000000000x", 49983)
			.await
			.expect_err("unknown")
			.is_not_found()
	);

	// Guest state that must survive pause/resume (tmpfs → memory snapshot).
	let vm = manager.live_vm(&id).unwrap();
	vm.agent()
		.fs_write("/tmp/marker", 0o644, b"before-pause")
		.await
		.unwrap();

	let pause = Instant::now();
	let paused = client
		.pause_sandbox(&id, &PauseSandbox { memory: true })
		.await
		.expect("pause");
	let pause_latency = pause.elapsed();
	assert_eq!(paused.state, SandboxState::Paused);
	assert!(client.open_tunnel(&id, 49983).await.is_err(), "paused sandbox has no tunnel");
	assert!(
		data
			.path()
			.join("paused")
			.join(&id)
			.join("base/current-generation")
			.is_file(),
		"a fork pauses as a delta against its hard-linked RAM base"
	);
	eprintln!("pause (delta memory snapshot + VMM kill): {pause_latency:?}");

	let resume = Instant::now();
	let resumed = client
		.resume_sandbox(&id, &ResumeSandbox {
			end_at_ms: now + 600_000,
			init,
			network: NetworkPolicy::default(),
			volume_mounts: Vec::new(),
			s3_mounts: Vec::new(),
			cold_boot: false,
			remote: None,
			meta: None,
			artifact_sources: Vec::new(),
		})
		.await
		.expect("resume");
	let resume_latency = resume.elapsed();
	assert_eq!(resumed.state, SandboxState::Running);
	assert_eq!(resumed.end_at_ms, now + 600_000);
	eprintln!("resume (fork paused snapshot + envd /init): {resume_latency:?}");
	assert_eq!(health_via_tunnel(client, &id).await, 204);
	let vm = manager.live_vm(&id).unwrap();
	assert_eq!(vm.agent().fs_read("/tmp/marker").await.unwrap(), b"before-pause");

	// Pause the resumed sandbox again: its RAM is the template base plus the
	// applied delta, and the next delta must carry both generations' writes.
	vm.agent()
		.fs_write("/tmp/marker2", 0o644, b"second-pause")
		.await
		.unwrap();
	client
		.pause_sandbox(&id, &PauseSandbox { memory: true })
		.await
		.expect("second pause");
	client
		.resume_sandbox(&id, &ResumeSandbox {
			end_at_ms:        now + 600_000,
			init:             init_template.clone(),
			network:          NetworkPolicy::default(),
			volume_mounts:    Vec::new(),
			s3_mounts:        Vec::new(),
			cold_boot:        false,
			remote:           None,
			meta:             None,
			artifact_sources: Vec::new(),
		})
		.await
		.expect("second resume");
	let vm = manager.live_vm(&id).unwrap();
	assert_eq!(vm.agent().fs_read("/tmp/marker").await.unwrap(), b"before-pause");
	assert_eq!(vm.agent().fs_read("/tmp/marker2").await.unwrap(), b"second-pause");

	let direct = EnvdClient::new(vm.clone())
		.metrics()
		.await
		.expect("direct envd metrics");
	assert!(direct.mem_total > 0, "envd reports memory");
	client
		.set_deadline(&id, now + 900_000)
		.await
		.expect("deadline");
	let deny_all = NetworkPolicy { deny_out: vec!["0.0.0.0/0".into()], ..NetworkPolicy::default() };
	client
		.update_network(&id, &deny_all)
		.await
		.expect("live network update");
	tokio::time::sleep(Duration::from_secs(6)).await;
	let samples = client.metrics(&id, None, None).await.expect("metrics");
	assert!(!samples.is_empty(), "envd metrics were scraped");
	assert!(samples[0].mem_total > 0);
	// Guest console output reaches the logs route (plain and structured lines).
	let echo = vmon_node::gc4::ExecRequest {
		cmd: vec![
			"/bin/sh".into(),
			"-c".into(),
			r#"echo plain-console-line > /dev/console; echo 'vmon-log:{"ts":1,"level":"error","msg":"structured-line"}' > /dev/console"#.into(),
		],
		..Default::default()
	};
	let (_, _, code) = vm
		.agent()
		.exec(&echo)
		.await
		.unwrap()
		.output()
		.await
		.unwrap();
	assert_eq!(code, 0);
	let query = |search: &str, level: Option<&str>| LogQuery {
		direction: Some("forward".into()),
		search: Some(search.into()),
		level: level.map(Into::into),
		..LogQuery::default()
	};
	// The 16550 console drains asynchronously; give the line time to land.
	let mut plain = Vec::new();
	for _ in 0..50 {
		plain = client
			.logs(&id, &query("plain-console-line", None))
			.await
			.expect("logs");
		if !plain.is_empty() {
			break;
		}
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	assert_eq!(plain.len(), 1, "{plain:?}");
	let structured = client
		.logs(&id, &query("structured-line", Some("error")))
		.await
		.expect("logs");
	assert_eq!(structured.len(), 1, "{structured:?}");
	assert_eq!(structured[0].level, "error");

	// Snapshot-as-build, then fork a second sandbox from it: guest memory
	// (tmpfs marker) carries over and the source keeps running.
	let snapshot_build = uuid::Uuid::new_v4().to_string();
	let snap = Instant::now();
	let artifact = client
		.snapshot_sandbox(&id, &SnapshotSandbox {
			build_id:    snapshot_build.clone(),
			template_id: "tple2e".into(),
			memory:      true,
		})
		.await
		.expect("snapshot sandbox");
	eprintln!("snapshot-as-build (pause + snapshot + rootfs clone + resume): {:?}", snap.elapsed());
	assert!(artifact.has_snapshot);
	assert_eq!(health_via_tunnel(client, &id).await, 204, "source still serves after snapshot");
	let child_id = "ie2etestsandbox000002".to_owned();
	let child_init = GuestInit { sandbox_id: child_id.clone(), ..init_template.clone() };
	let child = client
		.create_sandbox(&CreateSandbox {
			sandbox_id:       child_id.clone(),
			team_id:          "team".into(),
			template_id:      "tple2e".into(),
			build_id:         snapshot_build.clone(),
			artifact_sources: Vec::new(),
			vcpus:            1,
			memory_mb:        256,
			disk_mb:          512,
			end_at_ms:        now + 300_000,
			init:             child_init,
			network:          NetworkPolicy::default(),
			volume_mounts:    Vec::new(),
			s3_mounts:        Vec::new(),
			cold_boot:        false,
		})
		.await
		.expect("create from snapshot build");
	assert_eq!(child.build_id, snapshot_build);
	let child_vm = manager.live_vm(&child_id).unwrap();
	assert_eq!(child_vm.agent().fs_read("/tmp/marker").await.unwrap(), b"before-pause");
	client.kill_sandbox(&child_id).await.expect("kill child");

	// Volume mount forces a cold boot of the template rootfs; guest writes
	// land in the host volume directory.
	let volume_id = uuid::Uuid::new_v4().to_string();
	let volume_dir = data.path().join("volumes").join(&volume_id);
	std::fs::create_dir_all(&volume_dir).unwrap();
	let vol_id = "ie2etestsandbox000003".to_owned();
	let cold = Instant::now();
	client
		.create_sandbox(&CreateSandbox {
			sandbox_id:       vol_id.clone(),
			team_id:          "team".into(),
			template_id:      "tple2e".into(),
			build_id:         build_id.clone(),
			artifact_sources: Vec::new(),
			vcpus:            1,
			memory_mb:        256,
			disk_mb:          512,
			end_at_ms:        now + 300_000,
			init:             GuestInit { sandbox_id: vol_id.clone(), ..init_template.clone() },
			network:          NetworkPolicy::default(),
			volume_mounts:    vec![VolumeMount {
				volume_id:    volume_id.clone(),
				name:         "data".into(),
				path:         "/mnt/data".into(),
				object_store: false,
			}],
			s3_mounts:        Vec::new(),
			cold_boot:        false,
		})
		.await
		.expect("create with volume");
	eprintln!("create with volume (cold boot + virtiofs mount + envd /init): {:?}", cold.elapsed());
	let vol_vm = manager.live_vm(&vol_id).unwrap();
	let write = vmon_node::gc4::ExecRequest {
		cmd: vec!["/bin/sh".into(), "-c".into(), "echo from-guest > /mnt/data/hello && sync".into()],
		..Default::default()
	};
	let (_, stderr, code) = vol_vm
		.agent()
		.exec(&write)
		.await
		.unwrap()
		.output()
		.await
		.unwrap();
	assert_eq!(code, 0, "{}", String::from_utf8_lossy(&stderr));
	assert_eq!(std::fs::read_to_string(volume_dir.join("hello")).unwrap(), "from-guest\n");
	client
		.kill_sandbox(&vol_id)
		.await
		.expect("kill volume sandbox");

	let kill = Instant::now();
	client.kill_sandbox(&id).await.expect("kill");
	eprintln!("kill: {:?}", kill.elapsed());
	assert!(
		client
			.get_sandbox(&id)
			.await
			.expect_err("gone")
			.is_not_found()
	);
	assert_eq!(client.list_sandboxes().await.unwrap(), Vec::new());
	// Disks and paused state go in the kill's background teardown.
	let paused = data.path().join("paused").join(&id);
	let deadline = Instant::now() + Duration::from_secs(10);
	while paused.exists() {
		assert!(Instant::now() < deadline, "paused state of a killed sandbox was not removed");
		tokio::time::sleep(Duration::from_millis(20)).await;
	}
}

/// Guest workload that keeps block and rng interrupts in flight.
const TEMPLATE_IO_LOAD: &str = "while :; do dd if=/dev/vda of=/dev/null bs=4k count=256 \
                                2>/dev/null; head -c 4096 /dev/hwrng >/dev/null; done";

/// Regression: forks of a template snapshotted under interrupt load must all
/// become ready, also when created in parallel. On HVF a snapshot taken
/// while a vCPU was inside an interrupt handler restored with the handler's
/// GIC priority sometimes never dropped, so the fork never answered the agent
/// `hello` (the VMM pause now waits for vCPUs to leave handlers).
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_forks_all_become_ready() {
	if !e2e_enabled() {
		return;
	}
	let parallel: usize = std::env::var("VMON_E2E_PARALLEL")
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(4);
	let rounds: usize = std::env::var("VMON_E2E_ROUNDS")
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(3);
	let harness = harness(64, Some(TEMPLATE_IO_LOAD)).await;
	let now = vmon_node::sandbox::now_ms();
	for round in 0..rounds {
		let started = Instant::now();
		let creates = (0..parallel).map(|index| {
			let client = harness.client.clone();
			let build_id = harness.build_id.clone();
			let id = format!("iconc{round:02}x{index:014}");
			async move {
				let request = CreateSandbox {
					sandbox_id: id.clone(),
					team_id: "team".into(),
					template_id: "tple2e".into(),
					build_id,
					artifact_sources: Vec::new(),
					vcpus: 1,
					memory_mb: 256,
					disk_mb: 512,
					end_at_ms: now + 300_000,
					init: GuestInit {
						sandbox_id: id.clone(),
						template_id: "tple2e".into(),
						..GuestInit::default()
					},
					network: NetworkPolicy::default(),
					volume_mounts: Vec::new(),
					s3_mounts: Vec::new(),
					cold_boot: false,
				};
				let begun = Instant::now();
				let result = client.create_sandbox(&request).await;
				(id, begun.elapsed(), result)
			}
		});
		let results = futures::future::join_all(creates).await;
		let mut failures = Vec::new();
		for (id, elapsed, result) in &results {
			match result {
				Ok(_) => assert_eq!(health_via_tunnel(&harness.client, id).await, 204),
				Err(error) => failures.push(format!("{id} after {elapsed:?}: {error}")),
			}
		}
		eprintln!("round {round}: {parallel} creates in {:?}", started.elapsed());
		for (id, _, result) in &results {
			if result.is_ok() {
				harness.client.kill_sandbox(id).await.expect("kill");
			}
		}
		assert!(failures.is_empty(), "round {round} failures:\n{}", failures.join("\n"));
	}
}
