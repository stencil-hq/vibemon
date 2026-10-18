//! Real-VM template build tests (opt-in: `VMON_BUILD_E2E=1`).
//!
//! Needs `VMON_BIN` (a signed `vmon` that runs `vmon vmm …`),
//! `VMON_KERNEL`, `VMON_AGENT` (static Linux `vmon-agent`), network access
//! to Docker Hub, and a hypervisor. Builds templates from real images with
//! every step type, then forks a VM from the template snapshot and checks
//! the result inside the guest. `VMON_BUILD_E2E_ONLY=alpine|python|ubuntu`
//! runs a single case.

use std::{
	collections::BTreeMap,
	io::Write,
	path::PathBuf,
	sync::Arc,
	time::{Duration, Instant},
};

use axum::{Router, routing::get};
use base64::Engine as _;
use bytes::Bytes;
use http_body_util::Full;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use vmon_cluster::NodeId;

use super::{BuildConfig, Builder, Sources};
use crate::{
	api::{ArtifactInfo, BuildBase, BuildState, BuildStep, GuestInit, StartBuild, TemplateVersion},
	envd::{ENVD_PORT, EnvdClient},
	gc4::ExecRequest,
	net::{NetManager, NetMode},
	vm::{Boot, Disk, Vm, VmRuntime, VmRuntimeConfig, VmSpec, VmmIsolation},
};

struct Env {
	builder:     Builder,
	vms:         VmRuntime,
	data:        PathBuf,
	file_source: String,
	files_hash:  String,
}

fn enabled() -> bool {
	std::env::var("VMON_BUILD_E2E").is_ok_and(|v| v == "1")
}

fn path_var(name: &str) -> PathBuf {
	PathBuf::from(std::env::var(name).unwrap_or_else(|_| panic!("{name} must be set")))
}

/// tar.gz with `app/hello.txt` and a setuid `app/bin/suidtool`.
fn context_archive() -> Vec<u8> {
	let mut tar = tar::Builder::new(Vec::new());
	for (path, mode, data) in [
		("app/hello.txt", 0o644, &b"hello from COPY\n"[..]),
		("app/bin/suidtool", 0o4755, b"#!/bin/sh\nid -u\n"),
	] {
		let mut header = tar::Header::new_gnu();
		header.set_size(data.len() as u64);
		header.set_mode(mode);
		header.set_mtime(1);
		header.set_cksum();
		tar.append_data(&mut header, path, data).unwrap();
	}
	let mut gz = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
	gz.write_all(&tar.into_inner().unwrap()).unwrap();
	gz.finish().unwrap()
}

async fn setup() -> Env {
	let data = PathBuf::from(
		std::env::var("VMON_BUILD_E2E_DIR").unwrap_or_else(|_| "/tmp/vmon-build-e2e".into()),
	);
	setup_node(data, None).await
}

/// One node; `cpu_baseline` overrides the host's (to force snapshot
/// regeneration).
async fn setup_node(data: PathBuf, cpu_baseline: Option<&str>) -> Env {
	std::fs::create_dir_all(&data).unwrap();
	let net = NetManager::new(NetMode::Auto, "172.20.0.0/16", None).unwrap();
	let vms = VmRuntime::new(VmRuntimeConfig {
		vmon_bin: path_var("VMON_BIN"),
		run_dir: PathBuf::from(format!(
			"/tmp/vbe-{}-{}",
			std::process::id(),
			u8::from(cpu_baseline.is_some())
		)),
		boot_concurrency: 4,
		isolation: VmmIsolation::Default,
		net,
	})
	.unwrap();
	let baseline = std::process::Command::new(path_var("VMON_BIN"))
		.args(["vmm", "--print-cpu-baseline"])
		.output()
		.unwrap();
	let arch = if cfg!(target_arch = "aarch64") {
		"aarch64"
	} else {
		"x86_64"
	};
	let builder = Builder::new(
		BuildConfig {
			data_dir:        data.clone(),
			node_id:         NodeId(1),
			arch:            arch.into(),
			backend:         if cfg!(target_os = "macos") {
				"hvf".into()
			} else {
				"kvm".into()
			},
			cpu_baseline:    cpu_baseline.map_or_else(
				|| String::from_utf8_lossy(&baseline.stdout).trim().to_owned(),
				str::to_owned,
			),
			envd_version:    vmon_agent::envd::ENVD_VERSION.into(),
			kernel:          path_var("VMON_KERNEL"),
			agent_binary:    path_var("VMON_AGENT"),
			max_parallel:    2,
			step_cache_mb:   u64::MAX,
			secret:          "e2e-secret".into(),
			http:            reqwest::Client::new(),
			registry_mirror: std::env::var("VMON_REGISTRY_MIRROR").ok(),
			object_store:    None,
		},
		vms.clone(),
	)
	.unwrap();
	let archive = Arc::new(context_archive());
	let files_hash = hex::encode(Sha256::digest(&*archive));
	let app = Router::new().route(
		"/node/v1/files/{tid}/{hash}",
		get(move || {
			let archive = archive.clone();
			async move { archive.to_vec() }
		}),
	);
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
	let file_source = format!("http://{}", listener.local_addr().unwrap());
	tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
	Env { builder, vms, data, file_source, files_hash }
}

fn step(kind: &str, args: &[&str]) -> BuildStep {
	BuildStep {
		kind:       kind.into(),
		args:       args.iter().map(ToString::to_string).collect(),
		files_hash: None,
		force:      false,
	}
}

fn full_steps(env: &Env, install: &str) -> Vec<BuildStep> {
	let mut copy = step("COPY", &["app", "/opt/app", "", ""]);
	copy.files_hash = Some(env.files_hash.clone());
	vec![
		step("RUN", &[install, "root"]),
		copy,
		step("ENV", &["APP_DIR", "/opt/app"]),
		step("ENV", &["APP_BIN", "$APP_DIR/bin", "QUOTED", "a \"b\" $(echo no)"]),
		step("WORKDIR", &["/opt/app/work"]),
		step("USER", &["appuser"]),
		step("RUN", &["whoami > ~/whoami && pwd > ~/pwd && echo \"$APP_BIN\" > ~/app_bin"]),
		step("USER", &["user"]),
	]
}

async fn build(
	env: &Env,
	name: &str,
	from: BuildBase,
	steps: Vec<BuildStep>,
	start: Option<&str>,
	ready: Option<&str>,
) -> (ArtifactInfo, String) {
	let build_id = uuid::Uuid::new_v4().to_string();
	let started = Instant::now();
	let req = StartBuild {
		build_id: build_id.clone(),
		template_id: format!("e2e{name}"),
		team_id: "team".into(),
		from,
		steps,
		start_cmd: start.map(str::to_owned),
		ready_cmd: ready.map(str::to_owned),
		vcpus: 2,
		memory_mb: 1024,
		disk_mb: 4096,
		file_sources: Vec::new(),
		force: false,
		template_version: TemplateVersion::V2_1,
	};
	let sources = Sources { files: vec![env.file_source.clone()], parent: Vec::new() };
	env.builder.start(req, sources).unwrap();
	let mut offset = 0;
	loop {
		let status = env.builder.status(&build_id, offset).unwrap();
		for entry in &status.logs {
			let t = started.elapsed().as_secs_f64();
			println!(
				"[{name} {t:7.1}s] {:5} {:>8} {}",
				entry.level,
				entry.step.as_deref().unwrap_or("-"),
				entry.message
			);
		}
		offset += status.logs.len() as u64;
		match status.status {
			BuildState::Ready => {
				println!("[{name}] READY in {:.1}s", started.elapsed().as_secs_f64());
				return (status.artifact.unwrap(), build_id);
			},
			BuildState::Error | BuildState::Cancelled => {
				panic!("build {name} failed: {:?}", status.reason)
			},
			_ => tokio::time::sleep(Duration::from_millis(200)).await,
		}
	}
}

/// Forks a VM from the build's template snapshot (as a sandbox create would).
async fn fork(env: &Env, build_id: &str, info: &ArtifactInfo) -> (Vm, crate::vm::BuildNet) {
	let started = Instant::now();
	let dir = env.data.join("forks");
	std::fs::create_dir_all(&dir).unwrap();
	let overlay = dir.join(format!("{build_id}.img"));
	let _ = std::fs::remove_file(&overlay);
	let store = env.builder.artifacts();
	let name = format!("fork-{}", &build_id[..8]);
	let net = env.vms.build_network(&name).await.unwrap();
	let vm = env
		.vms
		.launch(VmSpec {
			name,
			boot: Boot::Fork { snapshot: store.snapshot_dir(build_id).unwrap() },
			vcpus: info.vcpus,
			memory_mb: info.memory_mb,
			disk: Some(Disk::Overlay { base: store.rootfs_path(build_id).unwrap(), path: overlay }),
			shares: Vec::new(),
			net: net.net.clone(),
			snapshot_root: None,
			console_log: dir.join(format!("{build_id}.console.log")),
			timeout: Some(Duration::from_mins(10)),
			ready_timeout: Duration::from_secs(30),
			extra_args: Vec::new(),
			gpu: None,
		})
		.await
		.unwrap();
	net.configure(&vm).await.unwrap();
	EnvdClient::new(vm.clone())
		.init(&GuestInit {
			env_vars:          BTreeMap::new(),
			envd_access_token: String::new(),
			default_user:      Some(info.default_user.clone()),
			default_workdir:   info.default_workdir.clone(),
			sandbox_id:        "ie2e".into(),
			template_id:       "e2e".into(),
			ca_bundle:         None,
		})
		.await
		.unwrap();
	println!(
		"[fork] forked + net + envd /init in {:.0}ms",
		started.elapsed().as_secs_f64() * 1000.0
	);
	(vm, net)
}

async fn sh(vm: &Vm, script: &str) -> String {
	let req = ExecRequest {
		cmd: vec!["/bin/sh".into(), "-c".into(), script.into()],
		env: BTreeMap::from([(
			"PATH".to_owned(),
			"/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin".to_owned(),
		)]),
		user: Some("root".into()),
		..ExecRequest::default()
	};
	let (stdout, stderr, code) = vm.agent().exec(&req).await.unwrap().output().await.unwrap();
	assert_eq!(code, 0, "`{script}` failed: {}", String::from_utf8_lossy(&stderr));
	String::from_utf8_lossy(&stdout).trim().to_owned()
}

/// Runs `script` through envd's `process.Process/Start` (Connect streaming,
/// JSON) without a user, i.e. as the sandbox's default user in its default
/// workdir; asserts exit code 0 and returns the trimmed stdout.
async fn envd_sh(vm: &Vm, script: &str) -> String {
	/// Connect envelope flag of the end-of-stream message.
	const END_STREAM: u8 = 0x02;
	let message = json!({"process": {"cmd": "/bin/sh", "args": ["-c", script]}}).to_string();
	let mut body = Vec::with_capacity(5 + message.len());
	body.push(0);
	body.extend_from_slice(&u32::try_from(message.len()).unwrap().to_be_bytes());
	body.extend_from_slice(message.as_bytes());
	let request = http::Request::post("/process.Process/Start")
		.header(http::header::HOST, format!("127.0.0.1:{ENVD_PORT}"))
		.header(http::header::CONTENT_TYPE, "application/connect+json")
		.body(Full::new(Bytes::from(body)))
		.unwrap();
	let response = EnvdClient::new(vm.clone()).request(request).await.unwrap();
	assert_eq!(response.status(), http::StatusCode::OK, "envd process.Process/Start");
	let (mut stdout, mut stderr, mut exit_code) = (Vec::new(), Vec::new(), None);
	let mut frames = &response.body()[..];
	while let [flags, a, b, c, d, rest @ ..] = frames {
		let len = usize::try_from(u32::from_be_bytes([*a, *b, *c, *d])).unwrap();
		let message: Value = serde_json::from_slice(&rest[..len]).unwrap();
		frames = &rest[len..];
		if flags & END_STREAM != 0 {
			assert!(message.get("error").is_none(), "`{script}` stream error: {message}");
			break;
		}
		let event = &message["event"];
		for (key, out) in [("stdout", &mut stdout), ("stderr", &mut stderr)] {
			if let Some(data) = event["data"][key].as_str() {
				out.extend(
					base64::engine::general_purpose::STANDARD
						.decode(data)
						.unwrap(),
				);
			}
		}
		if let Some(end) = event.get("end") {
			// Protobuf JSON omits a zero exit code.
			exit_code = Some(end["exitCode"].as_i64().unwrap_or(0));
		}
	}
	assert_eq!(
		exit_code,
		Some(0),
		"`{script}` via envd failed: {}",
		String::from_utf8_lossy(&stderr)
	);
	String::from_utf8_lossy(&stdout).trim().to_owned()
}

async fn tcp_get(vm: &Vm, port: u16, request: &[u8]) -> String {
	let mut stream = vm.tcp_connect(port).await.unwrap();
	stream.write_all(request).await.unwrap();
	let mut response = Vec::new();
	let _ = tokio::time::timeout(Duration::from_secs(5), stream.read_to_end(&mut response)).await;
	String::from_utf8_lossy(&response).into_owned()
}

async fn check_common(vm: &Vm, info: &ArtifactInfo) {
	assert_eq!(info.default_user, "user", "last USER wins");
	assert_eq!(info.default_workdir.as_deref(), Some("/opt/app/work"));
	assert_eq!(info.env["APP_BIN"], "/opt/app/bin", "ENV expands $VAR from earlier steps");
	assert_eq!(info.env["QUOTED"], "a \"b\" $(echo no)", "no command substitution");
	assert!(sh(vm, "id user").await.contains("uid="));
	assert!(sh(vm, "id appuser").await.contains("uid="));
	let sudo = sh(vm, "ls -l /usr/bin/sudo").await;
	assert!(sudo.starts_with("-rws"), "setuid sudo survives unpack: {sudo}");
	assert!(
		sh(vm, "grep -c 'user ALL=(ALL:ALL) NOPASSWD: ALL' /etc/sudoers")
			.await
			.parse::<u32>()
			.unwrap()
			>= 1
	);
	let tool = sh(vm, "stat -c '%A %U %G' /opt/app/bin/suidtool").await;
	assert_eq!(
		tool, "-rwsr-xr-x user user",
		"COPY applies the current USER as owner and keeps setuid"
	);
	assert_eq!(sh(vm, "cat /opt/app/hello.txt").await, "hello from COPY");
	assert_eq!(
		sh(vm, "cd /home/appuser && cat whoami pwd app_bin").await,
		"appuser\n/opt/app/work\n/opt/app/bin"
	);
	assert_eq!(
		sh(vm, "stat -c %U /opt/app/work").await,
		"user",
		"WORKDIR chowns created dirs to the current user (user phase)"
	);
	assert!(sh(vm, "cat /.e2b").await.contains("TEMPLATE_ID="));
	assert_eq!(sh(vm, "stat -c %a /code").await, "777");
}

/// `INSTALL_PROBE` of `templates/ubuntu-systemd/template.py`.
const UBUNTU_PROBE: &str = r"set -eu
cat > /etc/systemd/system/vmon-probe.service <<'EOF'
[Unit]
Description=vibemon probe: systemd starts enabled units at boot

[Service]
Type=oneshot
RemainAfterExit=yes
ExecStart=/bin/sh -c 'echo ok > /run/vmon-probe'

[Install]
WantedBy=multi-user.target
EOF
mkdir -p /etc/systemd/system/multi-user.target.wants
ln -sf ../vmon-probe.service /etc/systemd/system/multi-user.target.wants/vmon-probe.service
";

/// `INSTALL_HEARTBEAT` of `templates/ubuntu-systemd/template.py`.
const UBUNTU_HEARTBEAT: &str = r#"set -eu
cat > /usr/local/bin/vmon-heartbeat <<'EOF'
#!/bin/sh
# Start command of the ubuntu-systemd template: counts seconds into
# /tmp/vmon-heartbeat (replaced atomically) and records its pid.
echo $$ > /tmp/vmon-heartbeat.pid
i=0
while :; do
	i=$((i + 1))
	echo "$i" > /tmp/vmon-heartbeat.tmp
	mv -f /tmp/vmon-heartbeat.tmp /tmp/vmon-heartbeat
	sleep 1
done
EOF
chmod 0755 /usr/local/bin/vmon-heartbeat
"#;

/// Builds `templates/ubuntu-systemd` (same steps, start and ready commands)
/// from stock `ubuntu:24.04`, then checks a fork of its snapshot: systemd is
/// PID 1 and ran the enabled probe unit, envd's default user is `user`, and
/// the start command kept running across the snapshot.
async fn ubuntu_systemd(env: &Env) {
	let image = BuildBase::Image { image: "ubuntu:24.04".into(), registry: None };
	let steps = vec![
		step("RUN", &[UBUNTU_PROBE, "root"]),
		step("RUN", &[UBUNTU_HEARTBEAT, "root"]),
		step("USER", &["user"]),
		step("WORKDIR", &["/home/user"]),
	];
	let (info, build_id) = build(
		env,
		"ubuntu",
		image,
		steps,
		Some("/usr/local/bin/vmon-heartbeat"),
		Some("test -s /tmp/vmon-heartbeat"),
	)
	.await;
	assert_eq!(info.default_user, "user");
	assert_eq!(info.default_workdir.as_deref(), Some("/home/user"));
	let (vm, _net) = fork(env, &build_id, &info).await;
	assert_eq!(sh(&vm, ". /etc/os-release && echo \"$ID $VERSION_ID\"").await, "ubuntu 24.04");

	let init = sh(&vm, "readlink -f /proc/1/exe").await;
	assert!(init.ends_with("/systemd"), "systemd is PID 1, not {init}");
	assert_eq!(sh(&vm, "cat /proc/1/comm").await, "systemd");
	assert_eq!(sh(&vm, "systemctl is-active envd.service").await, "active");
	assert_eq!(sh(&vm, "systemctl is-enabled vmon-probe.service").await, "enabled");
	assert_eq!(sh(&vm, "systemctl is-active vmon-probe.service").await, "active");
	assert_eq!(sh(&vm, "cat /run/vmon-probe").await, "ok");

	// The image's own `ubuntu` account keeps uid 1000; `user` gets another.
	assert_eq!(sh(&vm, "id -u ubuntu").await, "1000");
	assert_ne!(sh(&vm, "id -u user").await, "1000");
	assert_eq!(
		envd_sh(&vm, "whoami; pwd").await,
		"user\n/home/user",
		"envd runs as the default user in the default workdir"
	);
	assert_eq!(envd_sh(&vm, "sudo -n true && echo sudo-ok").await, "sudo-ok");

	let pid = sh(&vm, "cat /tmp/vmon-heartbeat.pid").await;
	assert!(
		sh(&vm, &format!("tr '\\0' ' ' < /proc/{pid}/cmdline"))
			.await
			.contains("/usr/local/bin/vmon-heartbeat"),
		"the start command's process (pid {pid}) survives the snapshot"
	);
	assert_eq!(
		sh(&vm, &format!("stat -c %U /proc/{pid}")).await,
		"user",
		"the start command runs as the default user"
	);
	let beats = sh(
		&vm,
		"a=$(cat /tmp/vmon-heartbeat); sleep 3; b=$(cat /tmp/vmon-heartbeat); echo \"$a $b\"",
	)
	.await;
	let (before, after) = beats.split_once(' ').unwrap();
	assert!(
		after.parse::<u64>().unwrap() > before.parse::<u64>().unwrap(),
		"the heartbeat keeps counting in the fork: {beats}"
	);
	vm.kill().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn templates_build_snapshot_and_fork() {
	if !enabled() {
		eprintln!("skipped: set VMON_BUILD_E2E=1");
		return;
	}
	let env = setup().await;
	let only = std::env::var("VMON_BUILD_E2E_ONLY").unwrap_or_default();

	if only.is_empty() || only == "alpine" {
		let image = || BuildBase::Image { image: "alpine:3.20".into(), registry: None };
		let start = "socat TCP-LISTEN:8080,fork,reuseaddr SYSTEM:'echo hi-from-start'";
		let ready = "[ -n \"$(ss -Htuln sport = :8080)\" ]";
		let (info, build_id) = build(
			&env,
			"alpine",
			image(),
			full_steps(&env, "apk add --no-cache htop"),
			Some(start),
			Some(ready),
		)
		.await;
		let (vm, _net) = fork(&env, &build_id, &info).await;
		check_common(&vm, &info).await;
		assert_eq!(sh(&vm, "cat /proc/1/comm").await, "init", "busybox init (OpenRC) is PID 1");
		assert!(
			sh(&vm, "rc-status default 2>/dev/null | grep envd || true")
				.await
				.contains("envd")
		);
		assert!(sh(&vm, "command -v htop").await.ends_with("htop"));
		assert!(
			tcp_get(&vm, 8080, b"x\n").await.contains("hi-from-start"),
			"startCmd survives in the snapshot"
		);
		vm.kill().await;

		// Same spec again: every layer comes from the step cache.
		let (_, rebuilt) = build(
			&env,
			"alpine-cached",
			image(),
			full_steps(&env, "apk add --no-cache htop"),
			Some(start),
			Some(ready),
		)
		.await;
		let log = std::fs::read_to_string(
			env.builder
				.artifacts()
				.build_dir(&rebuilt)
				.unwrap()
				.join("build.log"),
		)
		.unwrap();
		assert!(log.contains("CACHED [builder 8/8] USER user"), "fully cached rebuild");

		// fromTemplate: inherits rootfs, defaults, and start command.
		let (child, child_id) = build(
			&env,
			"alpine-child",
			BuildBase::Build { build_id: build_id.clone(), sources: Vec::new() },
			vec![step("RUN", &["touch ~/child-was-here"])],
			None,
			None,
		)
		.await;
		assert_eq!(child.default_user, "user");
		let (vm, _net) = fork(&env, &child_id, &child).await;
		assert_eq!(sh(&vm, "stat -c %U /home/user/child-was-here").await, "user");
		assert!(tcp_get(&vm, 8080, b"x\n").await.contains("hi-from-start"), "inherited startCmd");
		vm.kill().await;

		// A second node with a different CPU baseline fetches the build from
		// this node's artifact routes and regenerates the snapshot locally.
		let app = Router::new()
			.nest("/node/v1", super::routes(env.builder.clone(), crate::server::Peers::default()));
		let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
		let source = format!("http://{}", listener.local_addr().unwrap());
		tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
		let peer = setup_node(env.data.join("peer"), Some("other-baseline")).await;
		let _ = peer.builder.artifacts().delete(&build_id).await;
		let started = Instant::now();
		let regenerated = peer.builder.ensure(&build_id, &[source]).await.unwrap();
		println!("[peer] fetched + regenerated snapshot in {:.1}s", started.elapsed().as_secs_f64());
		assert!(regenerated.has_snapshot);
		assert_eq!(regenerated.cpu_baseline, "other-baseline");
		assert_eq!(regenerated.default_workdir.as_deref(), Some("/opt/app/work"));
		let (vm, _net) = fork(&peer, &build_id, &regenerated).await;
		assert_eq!(sh(&vm, "cat /opt/app/hello.txt").await, "hello from COPY");
		assert!(
			tcp_get(&vm, 8080, b"x\n").await.contains("hi-from-start"),
			"regenerated snapshot runs startCmd"
		);
		vm.kill().await;
	}

	if only.is_empty() || only == "python" {
		let image = BuildBase::Image { image: "python:3.12-slim".into(), registry: None };
		let start = "python3 -m http.server 8080 --directory /opt/app";
		let ready =
			"curl -s -o /dev/null -w \"%{http_code}\" http://127.0.0.1:8080/ | grep -q \"200\"";
		let install = "apt-get update && DEBIAN_FRONTEND=noninteractive apt-get install -y \
		               --no-install-recommends tree";
		let (info, build_id) =
			build(&env, "python", image, full_steps(&env, install), Some(start), Some(ready)).await;
		let (vm, _net) = fork(&env, &build_id, &info).await;
		check_common(&vm, &info).await;
		assert_eq!(sh(&vm, "cat /proc/1/comm").await, "systemd", "systemd is PID 1");
		assert_eq!(sh(&vm, "systemctl is-active envd").await, "active");
		assert!(sh(&vm, "command -v tree").await.ends_with("tree"));
		let response = tcp_get(&vm, 8080, b"GET /hello.txt HTTP/1.0\r\n\r\n").await;
		assert!(
			response.contains("hello from COPY"),
			"startCmd http.server survives in the snapshot: {response}"
		);
		vm.kill().await;
	}

	if only.is_empty() || only == "ubuntu" {
		ubuntu_systemd(&env).await;
	}
}
