#![allow(dead_code, reason = "common test helpers may not be used by all test targets")]

pub mod agent;

use std::{
	fs,
	io::{self, BufRead, BufReader, Read, Write},
	os::unix::{fs::DirBuilderExt, net::UnixStream, process::CommandExt},
	path::{Path, PathBuf},
	process::{Child, Command, ExitStatus, Stdio},
	sync::{
		Arc, Mutex,
		atomic::{AtomicU32, Ordering},
	},
	thread::{self, JoinHandle},
	time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use flume::{Receiver, Sender};
use serde_json::{Value, json};

const TEST_ASSETS: &str = "target/test-assets";

/// Per-run scratch root. Follows `CARGO_TARGET_DIR` so a sudo run with an
/// isolated target dir (`just smoke-jail`) never leaves root-owned dirs in
/// the default `target/test-runs`.
fn test_runs_dir() -> PathBuf {
	let target = std::env::var("CARGO_TARGET_DIR").unwrap_or_else(|_| "target".into());
	Path::new(&target).join("test-runs")
}
const DEFAULT_MEM_MIB: &str = "256";

pub fn vmm() -> Command {
	let mut cmd = vmm_raw();
	if let Ok(action) = std::env::var("VMON_SECCOMP_ACTION") {
		cmd.arg("--seccomp-action").arg(action);
	}
	if matches!(std::env::var("VMON_JAIL"), Ok(ref v) if v == "1") {
		cmd.arg("--jail");
		let nanos = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		let id = format!("vm-{nanos}");
		let jail_root = test_runs_dir().join(format!("jail-{nanos}"));
		fs::create_dir_all(&jail_root).unwrap_or_else(|e| panic!("creating jail root: {e}"));
		let jail_root =
			fs::canonicalize(&jail_root).unwrap_or_else(|e| panic!("canonicalizing jail root: {e}"));
		cmd.arg("--id").arg(&id);
		cmd.arg("--jail-root").arg(&jail_root);
		let uid = std::env::var("VMON_SANDBOX_UID")
			.or_else(|_| std::env::var("SUDO_UID"))
			.unwrap_or_else(|_| "65534".to_string());
		let gid = std::env::var("VMON_SANDBOX_GID")
			.or_else(|_| std::env::var("SUDO_GID"))
			.unwrap_or_else(|_| "65534".to_string());
		cmd.arg("--sandbox-uid").arg(uid);
		cmd.arg("--sandbox-gid").arg(gid);
	}
	cmd
}

/// The `vmon` binary invoked with the `vmm` subcommand and NO env-derived
/// extra flags, for tests that construct jail/seccomp arguments themselves
/// (a duplicate flag is a clap error).
pub fn vmm_raw() -> Command {
	let mut cmd = Command::new(env!("CARGO_BIN_EXE_vmon"));
	cmd.arg("vmm");
	cmd
}

/// Hypervisor backend selected at compile time, mirroring the backend
/// `vmon` itself uses: KVM on Linux, Apple Hypervisor.framework on macOS.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Backend {
	/// Linux KVM (`/dev/kvm`).
	Kvm,
	/// macOS Hypervisor.framework on Apple Silicon.
	Hvf,
}

/// The hypervisor backend this test binary was built against.
pub const BACKEND: Backend = if cfg!(target_os = "macos") {
	Backend::Hvf
} else {
	Backend::Kvm
};

fn env_flag(name: &str) -> bool {
	matches!(std::env::var(name), Ok(v) if v == "1")
}

/// Whether end-to-end VM boots were requested (`VMON_E2E=1`) and the host
/// hypervisor is usable. Boot tests early-return when this is false so a plain
/// `cargo test` stays hermetic on machines without a hypervisor.
///
/// Covers all three supported environments: Linux/KVM, macOS/HVF, and a
/// Linux/KVM guest under Lima on macOS (which presents as Linux/KVM inside the
/// guest).
pub fn require_hv() -> bool {
	env_flag("VMON_E2E") && hv_present()
}

#[cfg(target_os = "linux")]
fn hv_present() -> bool {
	Path::new("/dev/kvm").exists()
}

#[cfg(target_os = "macos")]
fn hv_present() -> bool {
	// `kern.hv_support` is 1 on Macs where Hypervisor.framework can run.
	Command::new("sysctl")
		.args(["-n", "kern.hv_support"])
		.output()
		.is_ok_and(|o| o.stdout.starts_with(b"1"))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
fn hv_present() -> bool {
	false
}

/// Soak runs additionally require `VMON_SOAK=1`.
pub fn require_soak() -> bool {
	require_hv() && env_flag("VMON_SOAK")
}

/// Jail smoke tests need Linux, a hypervisor, and root: `--jail` refuses to
/// launch without euid 0. Skips noisily when `VMON_JAIL=1` asked for jail
/// coverage but the run is unprivileged.
pub fn require_jail() -> bool {
	if !require_hv() || !supports_linux_isolation() {
		return false;
	}
	// SAFETY: `geteuid` has no preconditions and only reads process credentials.
	let root = unsafe { libc::geteuid() } == 0;
	if !root && env_flag("VMON_JAIL") {
		eprintln!("SKIP jail tests: VMON_JAIL=1 but not running as root");
	}
	root
}

/// Linux TAP host networking (`--tap`); KVM only.
pub const fn supports_tap() -> bool {
	matches!(BACKEND, Backend::Kvm)
}

/// Entitlement-free user-mode NAT (`--net user`); macOS/HVF only.
pub const fn supports_user_net() -> bool {
	cfg!(target_os = "macos")
}

/// PCI virtio transport; `x86_64` only (aarch64 uses MMIO).
pub const fn supports_pci() -> bool {
	cfg!(target_arch = "x86_64")
}

/// userfaultfd RAM paging, namespace jail, and seccomp audit; Linux only.
pub const fn supports_linux_isolation() -> bool {
	cfg!(target_os = "linux")
}

#[cfg(target_arch = "x86_64")]
const KERNEL_ASSET: &str = "bzImage-x86_64";
#[cfg(target_arch = "aarch64")]
const KERNEL_ASSET: &str = "Image-aarch64";

#[cfg(target_arch = "x86_64")]
const INITRAMFS_ASSET: &str = "busybox-initramfs-x86_64.cpio.gz";
#[cfg(target_arch = "aarch64")]
const INITRAMFS_ASSET: &str = "busybox-initramfs-aarch64.cpio.gz";

#[cfg(target_arch = "x86_64")]
const ROOTFS_ASSET: &str = "rootfs-x86_64.ext4";
#[cfg(target_arch = "aarch64")]
const ROOTFS_ASSET: &str = "rootfs-aarch64.ext4";

/// Direct-boot kernel for the host architecture: raw ELF `vmlinux` on `x86_64`,
/// uncompressed arm64 `Image` on aarch64. `VMON_KERNEL` overrides the
/// bundled asset, e.g. a distro `Image` extracted from the host on Lima.
pub fn kernel() -> PathBuf {
	if let Some(path) = std::env::var_os("VMON_KERNEL") {
		let path = PathBuf::from(path);
		assert!(path.is_file(), "VMON_KERNEL points at missing file {}", path.display());
		return fs::canonicalize(&path).unwrap_or(path);
	}
	required_asset(KERNEL_ASSET)
}

/// busybox initramfs for the host architecture.
pub fn initramfs() -> PathBuf {
	required_asset(INITRAMFS_ASSET)
}

/// ext4 rootfs fixture for the host architecture.
pub fn rootfs() -> PathBuf {
	required_asset(ROOTFS_ASSET)
}

fn required_asset(name: &str) -> PathBuf {
	let path = Path::new(TEST_ASSETS).join(name);
	assert!(
		path.is_file(),
		"missing test asset {}; run demo/fetch-test-assets.sh before e2e tests",
		path.display()
	);
	fs::canonicalize(&path)
		.unwrap_or_else(|e| panic!("canonicalizing raw asset {}: {e}", path.display()))
}

/// Fresh private scratch dir `<test-runs>/<name>-<pid>-<seq>`. Kept short:
/// control and agent sockets inside it must fit `sun_path` (108 bytes) even
/// under a deep checkout path such as a CI runner's.
pub fn test_dir(name: &str) -> PathBuf {
	static SEQ: AtomicU32 = AtomicU32::new(0);
	let seq = SEQ.fetch_add(1, Ordering::Relaxed);
	let dir = test_runs_dir().join(format!("{name}-{}-{seq}", std::process::id()));
	// A previous run with the same pid may have left this dir behind.
	match fs::remove_dir_all(&dir) {
		Err(e) if e.kind() != io::ErrorKind::NotFound => {
			panic!("removing stale {}: {e}", dir.display());
		},
		_ => {},
	}
	// Private mode so control/agent sockets created here satisfy vmon's
	// 0700-or-stricter parent-directory requirement on every host.
	fs::DirBuilder::new()
		.recursive(true)
		.mode(0o700)
		.create(&dir)
		.unwrap_or_else(|e| panic!("creating {}: {e}", dir.display()));
	fs::canonicalize(&dir)
		.unwrap_or_else(|e| panic!("canonicalizing test_dir {}: {e}", dir.display()))
}

pub fn copy_rootfs(test_name: &str) -> PathBuf {
	let dir = test_dir(test_name);
	let dest = dir.join(ROOTFS_ASSET);
	fs::copy(rootfs(), &dest)
		.unwrap_or_else(|e| panic!("copying rootfs fixture to {}: {e}", dest.display()));
	dest
}

pub fn assert_snapshot_written(dir: &Path) {
	let current = dir.join("current-generation");
	let generation =
		fs::read_to_string(&current).unwrap_or_else(|e| panic!("reading {}: {e}", current.display()));
	let generation = generation.trim();
	assert!(
		dir.join(format!("vmstate.{generation}.bin")).is_file(),
		"snapshot generation {generation} state file missing in {}",
		dir.display()
	);
	assert!(
		dir.join(format!("memory.{generation}.bin")).is_file(),
		"snapshot generation {generation} memory file missing in {}",
		dir.display()
	);
}

pub fn current_memory_file_len(dir: &Path) -> u64 {
	let generation = fs::read_to_string(dir.join("current-generation"))
		.unwrap_or_else(|e| panic!("reading current-generation: {e}"));
	let generation = generation.trim();
	let path = dir.join(format!("memory.{generation}.bin"));
	fs::metadata(&path)
		.unwrap_or_else(|e| panic!("stat {}: {e}", path.display()))
		.len()
}

/// Test guest command line; mirrors the VMM's `DEFAULT_CMDLINE` (`x86_64` guest
/// KVM enables VMX/SVM only while the guest runs VMs, so a test VM that runs
/// nested in a sandbox stays snapshottable on Intel).
pub fn cmdline(mode: &str) -> String {
	let arch_args = if cfg!(target_arch = "x86_64") {
		" kvm.enable_virt_at_load=0"
	} else {
		""
	};
	format!("console=ttyS0 reboot=t panic=-1{arch_args} rdinit=/init vmon.test={mode}")
}

pub fn cmdline_with(mode: &str, extra: &[(&str, &str)]) -> String {
	let mut line = cmdline(mode);
	for (key, value) in extra {
		line.push(' ');
		line.push_str(key);
		line.push('=');
		line.push_str(value);
	}
	line
}

pub fn base_args(mode: &str) -> Vec<String> {
	vec![
		"--kernel".into(),
		kernel().display().to_string(),
		"--initrd".into(),
		initramfs().display().to_string(),
		"--mem".into(),
		DEFAULT_MEM_MIB.into(),
		"--cmdline".into(),
		cmdline(mode),
	]
}

pub fn base_args_with_cmdline(cmdline: String) -> Vec<String> {
	vec![
		"--kernel".into(),
		kernel().display().to_string(),
		"--initrd".into(),
		initramfs().display().to_string(),
		"--mem".into(),
		DEFAULT_MEM_MIB.into(),
		"--cmdline".into(),
		cmdline,
	]
}

pub fn as_refs(args: &[String]) -> Vec<&str> {
	args.iter().map(String::as_str).collect()
}

pub fn boot_capture(args: &[&str], marker: &str, timeout: Duration) -> String {
	boot_capture_with_input(args, b"\n", marker, timeout)
}

pub fn boot_capture_with_input(
	args: &[&str],
	serial_input: &[u8],
	marker: &str,
	timeout: Duration,
) -> String {
	let mut child = spawn_vmm_with_input(args, serial_input);
	let seen = child.wait_for(marker, timeout);
	child
		.wait_for_exit(Duration::from_secs(5))
		.map_or(seen, |(_, output)| output)
}

pub fn spawn_vmm(args: &[&str]) -> VmmProcess {
	spawn_vmm_with_input(args, b"")
}

pub fn spawn_vmm_with_input(args: &[&str], serial_input: &[u8]) -> VmmProcess {
	spawn_command_with_input(vmm(), args, serial_input)
}

/// Spawn with extra environment variables (e.g. `VMON_REMOTE_PAGE_TOKEN`).
pub fn spawn_vmm_with_env(args: &[&str], envs: &[(&str, &str)]) -> VmmProcess {
	let mut cmd = vmm();
	for (key, value) in envs {
		cmd.env(key, value);
	}
	spawn_command_with_input(cmd, args, b"")
}

/// Spawn `cmd` (a vmm invocation, e.g. [`vmm`] or [`vmm_raw`]) with `args`,
/// wiring serial stdin plus stdout/stderr capture readers.
pub fn spawn_command_with_input(
	mut cmd: Command,
	args: &[&str],
	serial_input: &[u8],
) -> VmmProcess {
	cmd.args(args)
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		// Own process group so cleanup can SIGKILL forked clones too (see kill()).
		.process_group(0);

	let mut child = cmd
		.spawn()
		.unwrap_or_else(|e| panic!("spawning vmon with args {args:?}: {e}"));

	if let Some(mut stdin) = child.stdin.take() {
		stdin
			.write_all(serial_input)
			.unwrap_or_else(|e| panic!("writing serial input to vmon: {e}"));
	}
	let (tx, rx) = flume::unbounded();
	let output = Arc::new(Mutex::new(Vec::new()));
	let mut readers = Vec::new();
	if let Some(stdout) = child.stdout.take() {
		readers.push(spawn_reader(stdout, Arc::clone(&output), tx.clone()));
	}
	if let Some(stderr) = child.stderr.take() {
		readers.push(spawn_reader(stderr, Arc::clone(&output), tx));
	}

	VmmProcess { child, output, notify: rx, readers }
}

fn spawn_reader<R>(mut reader: R, output: Arc<Mutex<Vec<u8>>>, notify: Sender<()>) -> JoinHandle<()>
where
	R: Read + Send + 'static,
{
	thread::spawn(move || {
		let mut buf = [0u8; 4096];
		loop {
			match reader.read(&mut buf) {
				Ok(0) => break,
				Ok(n) => {
					output
						.lock()
						.expect("output lock poisoned")
						.extend_from_slice(&buf[..n]);
					let _ = notify.send(());
				},
				Err(_) => break,
			}
		}
	})
}

pub struct VmmProcess {
	child:   Child,
	output:  Arc<Mutex<Vec<u8>>>,
	notify:  Receiver<()>,
	readers: Vec<JoinHandle<()>>,
}

impl VmmProcess {
	pub fn pid(&self) -> u32 {
		self.child.id()
	}

	pub fn output(&self) -> String {
		output_to_string(&self.output)
	}

	pub fn wait_for(&mut self, marker: &str, timeout: Duration) -> String {
		let deadline = Instant::now() + timeout;
		loop {
			let output = self.output();
			if output.contains(marker) {
				return output;
			}
			if let Some(status) = self
				.child
				.try_wait()
				.unwrap_or_else(|e| panic!("polling vmon status: {e}"))
			{
				// Join readers first: a guest that emits the marker and powers off
				// immediately can be reaped before the capture threads drained it.
				self.join_readers();
				let output = self.output();
				if output.contains(marker) {
					return output;
				}
				panic!("vmon exited with {status} before marker {marker:?}; output:\n{output}");
			}
			let now = Instant::now();
			if now >= deadline {
				let output = self.output();
				self.kill();
				panic!("timed out after {timeout:?} waiting for marker {marker:?}; output:\n{output}");
			}
			let wait = std::cmp::min(deadline - now, Duration::from_millis(50));
			let _ = self.notify.recv_timeout(wait);
		}
	}

	pub fn wait_for_exit(&mut self, timeout: Duration) -> Option<(ExitStatus, String)> {
		let deadline = Instant::now() + timeout;
		loop {
			if let Some(status) = self
				.child
				.try_wait()
				.unwrap_or_else(|e| panic!("polling vmon status: {e}"))
			{
				self.join_readers();
				return Some((status, self.output()));
			}
			let now = Instant::now();
			if now >= deadline {
				return None;
			}
			let wait = std::cmp::min(deadline - now, Duration::from_millis(50));
			let _ = self.notify.recv_timeout(wait);
		}
	}

	pub fn wait(&mut self, timeout: Duration) -> (ExitStatus, String) {
		if let Some(result) = self.wait_for_exit(timeout) {
			return result;
		}
		let output = self.output();
		self.kill();
		panic!("vmon did not exit within {timeout:?}; output:\n{output}");
	}

	pub fn has_exited(&mut self) -> Option<ExitStatus> {
		self
			.child
			.try_wait()
			.unwrap_or_else(|e| panic!("polling vmon status: {e}"))
	}

	pub fn kill(&mut self) {
		// SIGKILL the whole process group, not just the supervisor: `--fork-from`
		// clones inherit this process's stdout pipe, so a surviving clone would
		// keep it open and wedge join_readers() forever. Only signal while the
		// child is still running: once reaped, its pid (== the group pgid, set
		// via process_group(0)) could be recycled — and a normally-exited
		// supervisor has already waited for its clones, so none are orphaned.
		if matches!(self.child.try_wait(), Ok(None)) {
			// SAFETY: kill(2) targeting this live child's own process group.
			unsafe {
				libc::kill(-(self.child.id() as i32), libc::SIGKILL);
			}
		}
		let _ = self.child.wait();
		self.join_readers();
	}

	fn join_readers(&mut self) {
		for reader in self.readers.drain(..) {
			let _ = reader.join();
		}
	}
}

impl Drop for VmmProcess {
	fn drop(&mut self) {
		self.kill();
	}
}

fn output_to_string(output: &Arc<Mutex<Vec<u8>>>) -> String {
	String::from_utf8_lossy(&output.lock().expect("output lock poisoned")).into_owned()
}

pub struct ControlClient {
	reader:  BufReader<UnixStream>,
	writer:  UnixStream,
	next_id: u64,
}

impl ControlClient {
	pub fn connect(path: &Path, timeout: Duration) -> Self {
		let deadline = Instant::now() + timeout;
		loop {
			match UnixStream::connect(path) {
				Ok(stream) => {
					stream
						.set_read_timeout(Some(Duration::from_secs(10)))
						.expect("setting control read timeout");
					stream
						.set_write_timeout(Some(Duration::from_secs(10)))
						.expect("setting control write timeout");
					let writer = stream.try_clone().expect("cloning control stream");
					let mut client = Self { reader: BufReader::new(stream), writer, next_id: 1 };
					client.read_banner();
					return client;
				},
				Err(e)
					if matches!(
						e.kind(),
						io::ErrorKind::NotFound | io::ErrorKind::ConnectionRefused
					) && Instant::now() < deadline =>
				{
					thread::park_timeout(Duration::from_millis(10));
				},
				Err(e) => panic!("connecting to control socket {}: {e}", path.display()),
			}
		}
	}

	fn read_json_line(&mut self, context: &str) -> Value {
		let mut line = String::new();
		let n = self
			.reader
			.read_line(&mut line)
			.unwrap_or_else(|e| panic!("reading control {context}: {e}"));
		assert_ne!(n, 0, "control socket closed while reading {context}");
		serde_json::from_str(line.trim_end_matches(&['\r', '\n'][..]))
			.unwrap_or_else(|e| panic!("parsing control {context} JSON {line:?}: {e}"))
	}

	fn read_banner(&mut self) {
		let banner = self.read_json_line("banner");
		assert_eq!(
			banner.get("api").and_then(Value::as_u64),
			Some(1),
			"bad control banner: {banner}"
		);
		assert!(
			banner.get("vmm").and_then(Value::as_str).is_some(),
			"missing vmm banner version: {banner}"
		);
	}

	pub fn request(&mut self, method: &str, params: Value) -> Value {
		let id = self.next_id;
		self.next_id += 1;
		let req = json!({"id": id, "method": method, "params": params});
		self
			.writer
			.write_all(req.to_string().as_bytes())
			.unwrap_or_else(|e| panic!("writing control request {req}: {e}"));
		self
			.writer
			.write_all(b"\n")
			.unwrap_or_else(|e| panic!("terminating control request {req}: {e}"));
		self
			.writer
			.flush()
			.unwrap_or_else(|e| panic!("flushing control request {req}: {e}"));

		let reply = self.read_json_line("reply");
		assert_eq!(
			reply.get("id").and_then(Value::as_u64),
			Some(id),
			"control reply id mismatch: {reply}"
		);
		if reply.get("ok").and_then(Value::as_bool) == Some(true) {
			return reply.get("result").cloned().unwrap_or_else(|| json!({}));
		}
		panic!("control request {method} failed: {reply}");
	}

	pub fn request_error(&mut self, method: &str, params: Value) -> Value {
		let id = self.next_id;
		self.next_id += 1;
		let req = json!({"id": id, "method": method, "params": params});
		self
			.writer
			.write_all(req.to_string().as_bytes())
			.unwrap_or_else(|e| panic!("writing control error request {req}: {e}"));
		self
			.writer
			.write_all(b"\n")
			.unwrap_or_else(|e| panic!("terminating control error request {req}: {e}"));
		self
			.writer
			.flush()
			.unwrap_or_else(|e| panic!("flushing control error request {req}: {e}"));
		let reply = self.read_json_line("error reply");
		assert_eq!(
			reply.get("id").and_then(Value::as_u64),
			Some(id),
			"control error reply id mismatch: {reply}"
		);
		assert_eq!(
			reply.get("ok").and_then(Value::as_bool),
			Some(false),
			"expected error reply: {reply}"
		);
		reply
			.get("error")
			.cloned()
			.unwrap_or_else(|| panic!("missing error object in {reply}"))
	}

	pub fn raw_line(&mut self, line: &str) -> Value {
		self
			.writer
			.write_all(line.as_bytes())
			.unwrap_or_else(|e| panic!("writing raw control line {line:?}: {e}"));
		self
			.writer
			.write_all(b"\n")
			.unwrap_or_else(|e| panic!("terminating raw control line {line:?}: {e}"));
		self
			.writer
			.flush()
			.unwrap_or_else(|e| panic!("flushing raw control line {line:?}: {e}"));
		self.read_json_line("raw reply")
	}

	pub fn ping(&mut self) {
		let result = self.request("ping", json!({}));
		assert_eq!(result, json!({}));
	}

	pub fn command(&mut self, command: &str) -> String {
		let (method, params) = match command.split_once(' ') {
			Some(("snapshot", name)) => ("snapshot", json!({"name": name.trim()})),
			Some((method, _)) => (method, json!({})),
			None => (command, json!({})),
		};
		let _ = self.request(method, params);
		"OK".to_string()
	}
}

pub fn parse_env_usize(name: &str, default: usize) -> usize {
	std::env::var(name)
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(default)
}

pub fn parse_env_u64(name: &str, default: u64) -> u64 {
	std::env::var(name)
		.ok()
		.and_then(|v| v.parse().ok())
		.unwrap_or(default)
}

#[cfg(target_os = "linux")]
mod proc_stats {
	use std::fs;

	pub fn fd_count(pid: u32) -> Option<usize> {
		Some(fs::read_dir(format!("/proc/{pid}/fd")).ok()?.count())
	}

	pub fn rss_kb(pid: u32) -> Option<u64> {
		let status = fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
		status.lines().find_map(|line| {
			let rest = line.strip_prefix("VmRSS:")?;
			rest.split_whitespace().next()?.parse().ok()
		})
	}
}

#[cfg(not(target_os = "linux"))]
mod proc_stats {
	pub const fn fd_count(_pid: u32) -> Option<usize> {
		None
	}

	pub const fn rss_kb(_pid: u32) -> Option<u64> {
		None
	}
}

#[allow(unused_imports, reason = "conditional build target imports")]
pub use proc_stats::{fd_count, rss_kb};

pub fn assert_no_panic(output: &str) {
	let lower = output.to_ascii_lowercase();
	assert!(
		!lower.contains("kernel panic") && !lower.contains("panicked at"),
		"panic observed in vmon output:\n{output}"
	);
}
