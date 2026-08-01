//! Focused performance and benchmark suite.
//!
//! Gated by `VMON_E2E=1` + usable hypervisor + skopeo/umoci/mkfs.ext4.
//! Emits machine-readable phase breakdowns (template, restore, agent, exec),
//! p50/p95 latency metrics, and verifies high-density, leak-free 32-clone forks
//! against a sequential baseline.

mod common;

use std::{
	collections::HashSet,
	fs,
	path::{Path, PathBuf},
	process::Command,
	sync::Arc,
	time::{Duration, Instant},
};

use serde_json::{Value, json};
use vmon_proto::v1 as pb;
use vmond::{
	engine::{
		agent::AgentConn,
		spawn::{LaunchSpec, SandboxRuntime, SandboxVm, VmonRuntime},
	},
	home::Home,
	security::{EncryptedArchive, Keyring},
};

const EXTRA_PATH: &str =
	"/opt/homebrew/bin:/opt/homebrew/opt/e2fsprogs/sbin:/usr/local/bin:/usr/sbin:/sbin";

fn tool_path() -> String {
	let inherited = std::env::var("PATH").unwrap_or_default();
	format!("{EXTRA_PATH}:{inherited}")
}

fn have_tool(name: &str) -> bool {
	tool_path()
		.split(':')
		.any(|dir| !dir.is_empty() && Path::new(dir).join(name).is_file())
}

fn require_perf_e2e() -> bool {
	if !common::require_hv() {
		return false;
	}
	if std::env::var("VMON_E2E")
		.ok()
		.as_deref()
		.is_none_or(|value| value != "1" && value != "true")
	{
		eprintln!("SKIP perf_e2e: VMON_E2E=1 is not set");
		return false;
	}
	for tool in ["skopeo", "umoci", "mkfs.ext4"] {
		if !have_tool(tool) {
			eprintln!("SKIP perf_e2e: {tool} not found on PATH");
			return false;
		}
	}
	// Verify hypervisor usability via a quick dry launch of the server.
	let test_home = PathBuf::from(format!("/tmp/vp_probe_{}", std::process::id()));
	let _ = fs::remove_dir_all(&test_home);
	if fs::create_dir_all(&test_home).is_err() {
		return false;
	}
	let server = Server::start(&test_home);
	let grpc = server.grpc();
	let mut system = grpc.system();
	let res = grpc.block_on(system.info(pb::InfoRequest {}));
	let healthy = res.is_ok();

	let sandbox_ok = if healthy {
		let body = json!({
			"image": e2e_image(),
			"block_network": true,
			"memory": 256,
		});
		let mut sandboxes = grpc.sandboxes();
		let spec_json = body.to_string();
		let create_res =
			grpc.block_on(sandboxes.create(pb::CreateSandboxRequest { spec_json, no_wait: false }));
		if let Ok(res) = create_res {
			let view: Value = serde_json::from_str(&res.into_inner().json).unwrap();
			let rid = view["id"].as_str().unwrap().to_owned();
			let _ = grpc.block_on(sandboxes.remove(pb::SandboxRef { id: rid }));
			true
		} else {
			false
		}
	} else {
		false
	};

	let _ = fs::remove_dir_all(&test_home);
	if !sandbox_ok {
		eprintln!(
			"SKIP perf_e2e: hypervisor support is present but not fully functional or permitted on \
			 this host"
		);
		return false;
	}
	true
}

fn e2e_image() -> String {
	std::env::var("VMON_E2E_IMAGE").unwrap_or_else(|_| "alpine:latest".into())
}

fn cached_kernel() -> Option<PathBuf> {
	let name = if cfg!(target_arch = "aarch64") {
		"Image-aarch64"
	} else {
		"bzImage-x86_64"
	};
	let home = std::env::var("HOME").ok()?;
	let path = Path::new(&home).join(".vmon/assets").join(name);
	path.is_file().then_some(path)
}

struct Server {
	child: std::process::Child,
	home:  PathBuf,
	sock:  PathBuf,
	log:   PathBuf,
}

impl Server {
	fn start(home: &Path) -> Self {
		let log = home.join("server-perf.log");
		let log_file = fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&log)
			.expect("open server log");
		let mut cmd = Command::new(env!("CARGO_BIN_EXE_vmon"));
		cmd.arg("serve")
			.arg("--home")
			.arg(home)
			.env("PATH", tool_path())
			.stdin(std::process::Stdio::null())
			.stdout(log_file.try_clone().expect("clone log handle"))
			.stderr(log_file);
		if let Some(kernel) = cached_kernel() {
			cmd.env("VMON_KERNEL", kernel);
		}
		let child = cmd.spawn().expect("spawn vmon serve");
		let server = Self { child, home: home.to_path_buf(), sock: home.join("vmond.sock"), log };
		server.wait_healthy(Duration::from_secs(30));
		server
	}

	fn wait_healthy(&self, timeout: Duration) {
		let deadline = Instant::now() + timeout;
		loop {
			if self.sock.exists()
				&& let Ok(grpc) = common::api::Grpc::connect_uds(&self.sock)
			{
				let mut system = grpc.system();
				let res = grpc.block_on(system.info(pb::InfoRequest {}));
				if res.is_ok() {
					return;
				}
			}
			assert!(
				Instant::now() < deadline,
				"vmon serve never became healthy; log tail:\n{}",
				self.log_tail()
			);
			std::thread::sleep(Duration::from_millis(200));
		}
	}

	fn log_tail(&self) -> String {
		let text = fs::read_to_string(&self.log).unwrap_or_default();
		let lines: Vec<&str> = text.lines().collect();
		let start = lines.len().saturating_sub(40);
		lines[start..].join("\n")
	}

	fn grpc(&self) -> common::api::Grpc {
		common::api::Grpc::connect_uds(&self.sock).expect("connecting to server gRPC UDS")
	}

	fn pid(&self) -> u32 {
		self.child.id()
	}

	fn kill(&mut self) {
		if let Ok(vms) = SandboxVm::list_in(self.home.join("vms")) {
			for vm in vms {
				let _ = vm.stop(true);
			}
		}
		let _ = self.child.kill();
		let _ = self.child.wait();
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		self.kill();
	}
}

fn create_sandbox(server: &Server, extra: Value) -> Value {
	let mut body = json!({
		"image": e2e_image(),
		"block_network": true,
		"memory": 256,
	});
	if let (Value::Object(base), Value::Object(more)) = (&mut body, extra) {
		for (key, value) in more {
			base.insert(key, value);
		}
	}
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let view = grpc
		.block_on(
			sandboxes
				.create(pb::CreateSandboxRequest { spec_json: body.to_string(), no_wait: false }),
		)
		.unwrap_or_else(|status| {
			panic!(
				"create failed: {}; log tail:\n{}",
				common::api::status_detail(&status),
				server.log_tail()
			)
		})
		.into_inner();
	let view: Value = serde_json::from_str(&view.json).expect("create view JSON");
	assert!(view.get("id").and_then(Value::as_str).is_some(), "view missing id: {view}");
	view
}

fn exec(server: &Server, id: &str, cmd: &[&str]) -> (i64, String, String) {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let request = pb::ExecCaptureRequest {
		id:   id.to_owned(),
		exec: Some(pb::ExecStart {
			cmd: cmd.iter().map(|&part| part.to_owned()).collect(),
			timeout: Some(30.0),
			..Default::default()
		}),
	};
	let out = grpc
		.block_on(sandboxes.exec_capture(request))
		.unwrap_or_else(|status| {
			panic!("exec {cmd:?} failed: {}", common::api::status_detail(&status))
		})
		.into_inner();
	(
		out.code,
		String::from_utf8_lossy(&out.stdout).into_owned(),
		String::from_utf8_lossy(&out.stderr).into_owned(),
	)
}

fn remove_sandbox(server: &Server, id: &str) {
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	if let Err(status) = grpc.block_on(sandboxes.remove(pb::SandboxRef { id: id.to_owned() })) {
		assert_eq!(
			status.code(),
			tonic::Code::NotFound,
			"remove failed: {}",
			common::api::status_detail(&status)
		);
	}
}

fn process_resource_counts(pid: u32) -> (usize, usize) {
	#[cfg(target_os = "linux")]
	{
		let fds = fs::read_dir(format!("/proc/{pid}/fd"))
			.unwrap_or_else(|error| panic!("read server fd table: {error}"))
			.count();
		let status = fs::read_to_string(format!("/proc/{pid}/status"))
			.unwrap_or_else(|error| panic!("read server process status: {error}"));
		let threads = status
			.lines()
			.find_map(|line| {
				let rest = line.strip_prefix("Threads:")?;
				rest.split_whitespace().next()?.parse::<usize>().ok()
			})
			.expect("server thread count");
		(fds, threads)
	}
	#[cfg(target_os = "macos")]
	{
		let pid = pid.to_string();
		let lsof = Command::new("lsof")
			.args(["-n", "-P", "-a", "-p", &pid])
			.output()
			.expect("run lsof for server process");
		assert!(lsof.status.success(), "lsof failed: {}", String::from_utf8_lossy(&lsof.stderr));
		let fds = lsof
			.stdout
			.split(|byte| *byte == b'\n')
			.count()
			.saturating_sub(2);

		let ps = Command::new("ps")
			.args(["-M", "-p", &pid])
			.output()
			.expect("run ps for server process");
		assert!(ps.status.success(), "ps failed: {}", String::from_utf8_lossy(&ps.stderr));
		let threads = ps
			.stdout
			.split(|byte| *byte == b'\n')
			.count()
			.saturating_sub(2);
		(fds, threads)
	}
	#[cfg(not(any(target_os = "linux", target_os = "macos")))]
	{
		unreachable!("performance E2E only supports Linux and macOS")
	}
}

fn process_exists(pid: libc::pid_t) -> bool {
	// SAFETY: signal zero probes process existence without delivering a signal.
	let result = unsafe { libc::kill(pid, 0) };
	result == 0 || std::io::Error::last_os_error().raw_os_error() == Some(libc::EPERM)
}

fn unique_test_home(test_name: &str) -> PathBuf {
	use std::{
		collections::hash_map::DefaultHasher,
		hash::{Hash, Hasher},
	};
	let mut hasher = DefaultHasher::new();
	Instant::now().hash(&mut hasher);
	std::process::id().hash(&mut hasher);
	let num = hasher.finish();
	let path = PathBuf::from(format!("/tmp/vp_{test_name}_{num:016x}"));
	let _ = fs::remove_dir_all(&path);
	fs::create_dir_all(&path).unwrap();
	path
}

fn calculate_percentiles(mut latencies: Vec<Duration>) -> (Duration, Duration) {
	if latencies.is_empty() {
		return (Duration::ZERO, Duration::ZERO);
	}
	latencies.sort();
	let p50_idx = latencies.len() / 2;
	let p95_idx = (latencies.len() * 95) / 100;
	(latencies[p50_idx], latencies[p95_idx.min(latencies.len() - 1)])
}

#[test]
fn create_to_first_exec_latency_breakdown_and_p50_p95() {
	if !require_perf_e2e() {
		return;
	}
	let test_home = unique_test_home("latency");
	let server = Server::start(&test_home);
	let num_samples = 5;
	let mut total_latencies = Vec::new();
	let mut restore_latencies = Vec::new();
	let mut agent_latencies = Vec::new();
	let mut exec_latencies = Vec::new();

	// Measure the first create separately because it includes a cold image build.
	let t_cold_start = Instant::now();
	let warmup = create_sandbox(&server, json!({}));
	let cold_create_duration = t_cold_start.elapsed();

	let warmup_id = warmup["id"].as_str().unwrap().to_owned();
	let (exit, ..) = exec(&server, &warmup_id, &["/bin/sh", "-c", "echo hello"]);
	assert_eq!(exit, 0);
	remove_sandbox(&server, &warmup_id);

	println!("Cold create completed in {cold_create_duration:.2?}");

	// Creating snapshot from warm sandbox
	let snap_name = format!("perfsnap-{}", std::process::id());
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let warmup_base = create_sandbox(&server, json!({}));
	let base_id = warmup_base["id"].as_str().unwrap().to_owned();
	let _snap_res = grpc
		.block_on(sandboxes.snapshot(pb::SnapshotRequest {
			id:   base_id.clone(),
			name: Some(snap_name.clone()),
			stop: false,
		}))
		.unwrap();
	remove_sandbox(&server, &base_id);

	let snapshot_archive = test_home
		.join("snapshots")
		.join(format!("{snap_name}.venc"));
	let keyring = Keyring::open(&Home::new(&test_home)).unwrap();
	let snapshot_dir =
		EncryptedArchive::open(&snapshot_archive, &test_home.join("perf-snapshot"), &keyring)
			.unwrap();
	let runtime = VmonRuntime;

	for i in 0..num_samples {
		let rid = format!("perf-sample-{i}");
		let vm_dir = test_home.join("vms").join(&rid);
		let vm = SandboxVm::from_dir(rid.clone(), vm_dir);

		let mut spec = LaunchSpec::restore(vm.api_sock(), &snapshot_dir)
			.with_agent_sock(vm.dir().join("agent.sock"));
		let base_disk = snapshot_dir.join("rootfs.img");
		if base_disk.is_file() {
			spec = spec.with_disk_overlay(base_disk, vm.dir().join("rootfs.img"));
		}

		let t_total_start = Instant::now();
		// Measure Phase 2: VM Restore Latency (Wait for api.sock readiness)
		let t_restore_start = Instant::now();
		runtime.launch(&vm, &spec).unwrap();
		let restore_duration = t_restore_start.elapsed();
		restore_latencies.push(restore_duration);

		// Measure Phase 3: Agent Connection Readiness Latency (Connect + Ping)
		let t_agent_start = Instant::now();
		let agent_sock = vm.dir().join("agent.sock");
		let agent = AgentConn::connect(&agent_sock, Duration::from_secs(10)).unwrap();
		agent.ping(Duration::from_secs(10)).unwrap();
		let agent_duration = t_agent_start.elapsed();
		agent_latencies.push(agent_duration);

		// Measure Phase 4: First Command Execution Latency (Wait for Exit)
		let t_exec_start = Instant::now();
		let session = agent
			.exec(&["/bin/sh", "-c", "echo ready"], None, None, false, None)
			.unwrap();
		let exit_code = session.wait(Some(Duration::from_secs(10))).unwrap();
		assert_eq!(exit_code, 0);
		let exec_duration = t_exec_start.elapsed();
		exec_latencies.push(exec_duration);
		let total_duration = t_total_start.elapsed();
		total_latencies.push(total_duration);

		// Cleanup sample
		agent.close();
		let _ = runtime.stop(&vm, true);
		let _ = runtime.remove(&vm);

		println!(
			"Sample {}: restore={:.2?}, agent_ready={:.2?}, first_exec={:.2?}, total={:.2?}",
			i + 1,
			restore_duration,
			agent_duration,
			exec_duration,
			total_duration
		);
	}

	let (total_p50, total_p95) = calculate_percentiles(total_latencies);
	let (restore_p50, restore_p95) = calculate_percentiles(restore_latencies);
	let (agent_p50, agent_p95) = calculate_percentiles(agent_latencies);
	let (exec_p50, exec_p95) = calculate_percentiles(exec_latencies);

	let metrics = json!({
		"kind": "create_to_first_exec_latency",
		"samples": num_samples,
		"cold_create_ms": cold_create_duration.as_millis(),
		"warm_create_to_first_exec": {
			"p50_ms": total_p50.as_millis(),
			"p95_ms": total_p95.as_millis(),
		},
		"phases": {
			"vm_restore": {
				"p50_ms": restore_p50.as_millis(),
				"p95_ms": restore_p95.as_millis(),
			},
			"agent_ready": {
				"p50_ms": agent_p50.as_millis(),
				"p95_ms": agent_p95.as_millis(),
			},
			"first_exec": {
				"p50_ms": exec_p50.as_millis(),
				"p95_ms": exec_p95.as_millis(),
			}
		}
	});

	println!("{}", serde_json::to_string_pretty(&metrics).unwrap());
}

#[test]
fn high_density_32_clone_fork_correctness_and_no_leak_verification() {
	if !require_perf_e2e() {
		return;
	}
	let test_home = unique_test_home("density");
	let server = Server::start(&test_home);

	// Pre-build Alpine template snapshot
	let warmup = create_sandbox(&server, json!({}));
	let base_id = warmup["id"].as_str().unwrap().to_owned();
	let snap_name = format!("forktemplate-{}", std::process::id());
	let grpc = server.grpc();
	let mut sandboxes = grpc.sandboxes();
	let _snap_res = grpc
		.block_on(sandboxes.snapshot(pb::SnapshotRequest {
			id:   base_id.clone(),
			name: Some(snap_name.clone()),
			stop: false,
		}))
		.unwrap();
	remove_sandbox(&server, &base_id);

	// Fork 32 clones and track the long-lived server rather than this test process.
	let server_pid = server.pid();
	let (fd_before, threads_before) = process_resource_counts(server_pid);

	let mut snapshots = grpc.snapshots();
	let t_fork_start = Instant::now();
	let fork_res = grpc
		.block_on(snapshots.fork(pb::ForkSnapshotRequest {
			name:      snap_name,
			body_json: json!({"count": 32}).to_string(),
		}))
		.unwrap()
		.into_inner();
	let t_fork_done = Instant::now();

	let fork_val: Value = serde_json::from_str(&fork_res.json).unwrap();
	let clones = fork_val["clones"].as_array().unwrap();
	assert_eq!(clones.len(), 32, "Expected exactly 32 fork clones");

	println!(
		"Forked 32 clones in {:.2?}. Executing concurrent tests...",
		t_fork_done.duration_since(t_fork_start)
	);

	// Canonical sandbox views always carry the supervisor PID. Require one
	// for every clone so the later post-cleanup leak check is authoritative.
	let clone_pids = clones
		.iter()
		.map(|clone| {
			let pid = clone["pid"]
				.as_i64()
				.and_then(|pid| i32::try_from(pid).ok())
				.unwrap_or_else(|| panic!("fork clone missing valid canonical pid: {clone}"));
			assert!(pid > 0, "fork clone has non-positive canonical pid: {clone}");
			pid
		})
		.collect::<Vec<_>>();
	assert_eq!(clone_pids.len(), 32);
	assert_eq!(clone_pids.iter().copied().collect::<HashSet<_>>().len(), 32);
	assert!(
		clone_pids.iter().copied().all(process_exists),
		"one or more fork supervisors exited before execution"
	);

	let mut sequential_durations = Vec::with_capacity(clones.len());
	let sequential_start = Instant::now();
	for clone in clones {
		let cid = clone["id"].as_str().unwrap();
		let started = Instant::now();
		let (exit, stdout, _) = exec(&server, cid, &["/bin/sh", "-c", "printf baseline"]);
		sequential_durations.push(started.elapsed());
		assert_eq!(exit, 0, "sequential baseline failed for {cid}");
		assert_eq!(stdout, "baseline", "unexpected sequential output from {cid}");
	}
	let sequential_total = sequential_start.elapsed();
	let (sequential_p50, sequential_p95) = calculate_percentiles(sequential_durations);

	let parallel_start = Instant::now();

	// Verify isolation and responsive execution in parallel
	use std::sync::mpsc;
	let (tx, rx) = mpsc::channel();
	let server_ref = Arc::new(server);

	let exec_handles: Vec<_> = clones
		.iter()
		.map(|clone| {
			let cid = clone["id"].as_str().unwrap().to_owned();
			let s_ref = Arc::clone(&server_ref);
			let tx = tx.clone();
			std::thread::spawn(move || {
				let started = Instant::now();
				let command = [
					"/bin/sh",
					"-c",
					"printf '%s' \"$1\" > /tmp/vmon-fork-id; cat /tmp/vmon-fork-id",
					"sh",
					cid.as_str(),
				];
				let (exit, stdout, _) = exec(&s_ref, &cid, &command);
				tx.send((cid, exit, stdout, started.elapsed())).unwrap();
			})
		})
		.collect();

	for h in exec_handles {
		h.join().unwrap();
	}

	let mut exec_durations = Vec::new();
	for _ in 0..32 {
		let (cid, exit, stdout, duration) = rx.recv().unwrap();
		assert_eq!(exit, 0, "clone {cid} execution failed");
		assert_eq!(stdout, cid, "clone {cid} observed another clone's writable state");
		exec_durations.push(duration);
	}

	let parallel_total = parallel_start.elapsed();
	let (exec_p50, exec_p95) = calculate_percentiles(exec_durations);

	println!(
		"High-density validation: fork={:.2?}, sequential={:.2?}, parallel={:.2?}",
		t_fork_done.duration_since(t_fork_start),
		sequential_total,
		parallel_total
	);

	// Cleanup all clones
	for clone in clones {
		let cid = clone["id"].as_str().unwrap();
		remove_sandbox(&server_ref, cid);
	}

	let deadline = Instant::now() + Duration::from_secs(5);
	while clone_pids.iter().copied().any(process_exists) && Instant::now() < deadline {
		std::thread::sleep(Duration::from_millis(20));
	}
	let leaked_pids = clone_pids
		.iter()
		.copied()
		.filter(|pid| process_exists(*pid))
		.collect::<Vec<_>>();
	assert!(leaked_pids.is_empty(), "clone supervisors survived removal: {leaked_pids:?}");

	let resource_deadline = Instant::now() + Duration::from_secs(15);
	let (fd_after, threads_after) = loop {
		let counts = process_resource_counts(server_pid);
		if (counts.0 <= fd_before + 3 && counts.1 <= threads_before)
			|| Instant::now() >= resource_deadline
		{
			break counts;
		}
		std::thread::sleep(Duration::from_millis(20));
	};

	let report = json!({
		"kind": "fork_density",
		"clones": 32,
		"fork_ms": t_fork_done.duration_since(t_fork_start).as_millis(),
		"sequential_exec": {
			"total_ms": sequential_total.as_millis(),
			"p50_ms": sequential_p50.as_millis(),
			"p95_ms": sequential_p95.as_millis(),
		},
		"parallel_exec": {
			"total_ms": parallel_total.as_millis(),
			"p50_ms": exec_p50.as_millis(),
			"p95_ms": exec_p95.as_millis(),
		},
		"resource_growth": {
			"fds_before": fd_before,
			"fds_after": fd_after,
			"fds_delta": fd_after.saturating_sub(fd_before),
			"threads_before": threads_before,
			"threads_after": threads_after,
			"threads_delta": threads_after.saturating_sub(threads_before),
		}
	});
	println!("{}", serde_json::to_string_pretty(&report).unwrap());

	assert!(
		fd_after <= fd_before + 3,
		"server descriptor leak: before={fd_before}, after={fd_after}"
	);
	assert!(
		threads_after <= threads_before,
		"server thread leak: before={threads_before}, after={threads_after}"
	);
}
