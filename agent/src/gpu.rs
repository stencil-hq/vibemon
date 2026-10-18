//! NVIDIA GPU bring-up for GPU sandboxes.
//!
//! A GPU sandbox gets a whole NVIDIA GPU through VFIO plus the node's GPU
//! driver bundle (built by `kernel/nvidia.sh` for this guest kernel) as the
//! read-only virtio-fs share [`SHARE_TAG`]. At startup, before envd serves,
//! [`init`] mounts the bundle at [`MOUNT_POINT`], points kernel firmware
//! loading at it, loads `nvidia.ko` and `nvidia-uvm.ko`, creates the device
//! nodes `nvidia-modprobe` would, makes the libraries resolvable the way the
//! NVIDIA Container Toolkit does (an `ld.so.conf.d` entry plus `ldconfig`),
//! and links the tools (`nvidia-smi`, …) into `/usr/bin`. Failures are logged,
//! never fatal; guests without an NVIDIA GPU skip all of it.

use std::{
	ffi::CString,
	fs::{self, File},
	io,
	os::{
		fd::AsRawFd,
		unix::{
			ffi::OsStrExt,
			fs::{FileTypeExt, MetadataExt, PermissionsExt, symlink},
		},
	},
	path::Path,
	process::Command,
	thread,
	time::{Duration, Instant},
};

use serde::Deserialize;
use serde_json::{Value, json};

use crate::logging::{self, Level};

/// virtio-fs tag of the GPU driver bundle share (VMM tags are `[a-z0-9_]`).
pub const SHARE_TAG: &str = "vmon_gpu";
/// Where the bundle is mounted (read-only).
pub const MOUNT_POINT: &str = "/run/vmon/gpu";

const PCI_DEVICES: &str = "/sys/bus/pci/devices";
const NVIDIA_VENDOR: &str = "0x10de";
/// PCI base class 0x03: display controller (VGA and 3D controllers).
const DISPLAY_CLASS_PREFIX: &str = "0x03";
const FIRMWARE_PATH_PARAM: &str = "/sys/module/firmware_class/parameters/path";
/// Load order: `nvidia-uvm` links against `nvidia`.
const MODULES: [&str; 2] = ["nvidia", "nvidia-uvm"];
/// Character major of `/dev/nvidiactl` and `/dev/nvidia<N>` (fixed by the
/// driver).
const NVIDIA_MAJOR: u32 = 195;
const NVIDIACTL_MINOR: u32 = 255;
const NVIDIA_GPUS_PROC: &str = "/proc/driver/nvidia/gpus";
const LD_CONF: &str = "/etc/ld.so.conf.d/vmon-nvidia.conf";
const LDCONFIG: [&str; 3] = ["/sbin/ldconfig", "/usr/sbin/ldconfig", "/usr/bin/ldconfig"];
const TOOL_DIR: &str = "/usr/bin";
/// Unload order: dependents first (`nvidia-drm` and `nvidia-modeset` are not
/// in the bundle but are unloaded too if a template loaded its own).
const UNLOAD: [&str; 4] = ["nvidia_drm", "nvidia_modeset", "nvidia_uvm", "nvidia"];
/// Path prefix of the driver's device nodes.
const DEVICE_PREFIX: &str = "/dev/nvidia";
/// Upper bound for killing GPU users and unloading the modules.
const UNLOAD_WAIT: Duration = Duration::from_secs(10);

/// `manifest.json` at the bundle root.
#[derive(Deserialize)]
struct Manifest {
	driver: String,
	kernel: String,
}

/// Loads the NVIDIA driver when the VM has an NVIDIA display-class PCI
/// function. Idempotent: an agent restart finds the modules and nodes in place.
pub fn init() {
	let gpus = nvidia_gpus();
	if gpus.is_empty() {
		logging::emit(Level::Debug, "no NVIDIA GPU; skipping GPU driver setup", Value::Null);
		return;
	}
	let started = Instant::now();
	match bring_up() {
		Ok((driver, devices)) => {
			logging::emit(
				Level::Info,
				"NVIDIA GPU driver loaded",
				json!({
					"gpus": gpus,
					"driver": driver,
					"devices": devices,
					"elapsed_ms": started.elapsed().as_millis(),
				}),
			);
		},
		Err(err) => logging::emit(
			Level::Error,
			"NVIDIA GPU driver setup failed",
			json!({"gpus": gpus, "error": err}),
		),
	}
}

/// PCI addresses of NVIDIA display-class functions.
fn nvidia_gpus() -> Vec<String> {
	let Ok(entries) = fs::read_dir(PCI_DEVICES) else {
		return Vec::new();
	};
	let attr = |dir: &Path, name: &str| fs::read_to_string(dir.join(name)).unwrap_or_default();
	let mut gpus: Vec<String> = entries
		.flatten()
		.filter(|entry| {
			let dir = entry.path();
			attr(&dir, "vendor").trim() == NVIDIA_VENDOR
				&& attr(&dir, "class").trim().starts_with(DISPLAY_CLASS_PREFIX)
		})
		.map(|entry| entry.file_name().to_string_lossy().into_owned())
		.collect();
	gpus.sort();
	gpus
}

/// Mount, load, and expose the driver. Returns the driver version and the
/// per-GPU device nodes.
fn bring_up() -> Result<(String, Vec<String>), String> {
	let root = Path::new(MOUNT_POINT);
	let manifest = mount_bundle(root)?;
	let release = fs::read_to_string("/proc/sys/kernel/osrelease")
		.map_err(|err| format!("read kernel release: {err}"))?;
	if manifest.kernel != release.trim() {
		return Err(format!(
			"bundle driver {} was built for kernel {}, this guest runs {}",
			manifest.driver,
			manifest.kernel,
			release.trim()
		));
	}
	fs::write(FIRMWARE_PATH_PARAM, format!("{MOUNT_POINT}/firmware"))
		.map_err(|err| format!("set {FIRMWARE_PATH_PARAM}: {err}"))?;
	for module in MODULES {
		load_module(&root.join("modules").join(format!("{module}.ko")))?;
	}
	let devices = create_device_nodes()?;
	if let Err(err) = register_libraries(&root.join("lib")) {
		logging::emit(Level::Warn, "NVIDIA libraries not registered", json!({"error": err}));
	}
	link_tools(&root.join("bin"));
	for device in &devices {
		check_initialized(device)?;
	}
	Ok((manifest.driver, devices))
}

/// Opens a GPU's device node once. The driver initializes GPUs at probe time
/// (booting the GSP firmware inside `finit_module`) and keeps them up without
/// clients; a GPU whose probe-time init failed answers `EIO` here.
fn check_initialized(device: &str) -> Result<(), String> {
	File::open(device).map(drop).map_err(|err| {
		format!("{device}: {err}: GPU initialization failed (see NVRM lines in dmesg)")
	})
}

/// Mounts the bundle share read-only unless an earlier agent run did, and
/// reads its manifest.
fn mount_bundle(root: &Path) -> Result<Manifest, String> {
	let manifest_path = root.join("manifest.json");
	if !manifest_path.exists() {
		fs::create_dir_all(root).map_err(|err| format!("create {MOUNT_POINT}: {err}"))?;
		let source = CString::new(SHARE_TAG).map_err(|err| err.to_string())?;
		let target = CString::new(MOUNT_POINT).map_err(|err| err.to_string())?;
		// Executable (nvidia-smi, mmap'd libraries); no device nodes or setuid.
		let flags = libc::MS_RDONLY | libc::MS_NOSUID | libc::MS_NODEV;
		// SAFETY: all strings are nul-terminated and outlive the call; virtiofs
		// takes no mount data.
		let rc = unsafe {
			libc::mount(
				source.as_ptr(),
				target.as_ptr(),
				c"virtiofs".as_ptr(),
				flags,
				std::ptr::null(),
			)
		};
		if rc != 0 {
			return Err(format!(
				"mount virtio-fs tag {SHARE_TAG} at {MOUNT_POINT}: {} (the node must share the GPU \
				 driver bundle)",
				io::Error::last_os_error()
			));
		}
	}
	let bytes =
		fs::read(&manifest_path).map_err(|err| format!("read {}: {err}", manifest_path.display()))?;
	serde_json::from_slice(&bytes).map_err(|err| format!("parse {}: {err}", manifest_path.display()))
}

/// `finit_module(2)` without parameters; an already loaded module is success.
fn load_module(path: &Path) -> Result<(), String> {
	let file = File::open(path).map_err(|err| format!("open {}: {err}", path.display()))?;
	// SAFETY: the fd stays open for the call, the parameter string is
	// nul-terminated, and flags 0 asks for no special handling.
	let rc = unsafe { libc::syscall(libc::SYS_finit_module, file.as_raw_fd(), c"".as_ptr(), 0) };
	if rc == 0 {
		return Ok(());
	}
	let err = io::Error::last_os_error();
	if err.raw_os_error() == Some(libc::EEXIST) {
		Ok(())
	} else {
		Err(format!("load {}: {err} (see dmesg)", path.display()))
	}
}

/// `/dev/nvidiactl`, `/dev/nvidia<minor>` per GPU, and the UVM nodes. Returns
/// the per-GPU nodes.
fn create_device_nodes() -> Result<Vec<String>, String> {
	char_device("/dev/nvidiactl", NVIDIA_MAJOR, NVIDIACTL_MINOR)?;
	let mut devices = Vec::new();
	for minor in gpu_minors()? {
		let path = format!("/dev/nvidia{minor}");
		char_device(&path, NVIDIA_MAJOR, minor)?;
		devices.push(path);
	}
	let uvm = char_major("nvidia-uvm")?;
	char_device("/dev/nvidia-uvm", uvm, 0)?;
	char_device("/dev/nvidia-uvm-tools", uvm, 1)?;
	Ok(devices)
}

/// Device minors from `/proc/driver/nvidia/gpus/<bdf>/information`
/// (`Device Minor: N`).
fn gpu_minors() -> Result<Vec<u32>, String> {
	let entries =
		fs::read_dir(NVIDIA_GPUS_PROC).map_err(|err| format!("read {NVIDIA_GPUS_PROC}: {err}"))?;
	let mut minors = Vec::new();
	for entry in entries.flatten() {
		let info = fs::read_to_string(entry.path().join("information")).unwrap_or_default();
		if let Some(minor) = info.lines().find_map(|line| {
			line
				.strip_prefix("Device Minor:")
				.and_then(|value| value.trim().parse().ok())
		}) {
			minors.push(minor);
		}
	}
	if minors.is_empty() {
		return Err(format!("the driver probed no GPU (no {NVIDIA_GPUS_PROC}/*); see dmesg"));
	}
	minors.sort_unstable();
	Ok(minors)
}

/// Dynamic character major registered under `name` in `/proc/devices`.
fn char_major(name: &str) -> Result<u32, String> {
	let devices =
		fs::read_to_string("/proc/devices").map_err(|err| format!("read /proc/devices: {err}"))?;
	devices
		.lines()
		.skip_while(|line| !line.starts_with("Character devices:"))
		.take_while(|line| !line.is_empty())
		.find_map(|line| {
			let (major, dev) = line.trim().split_once(' ')?;
			(dev == name).then(|| major.parse().ok()).flatten()
		})
		.ok_or_else(|| format!("no character device {name} in /proc/devices"))
}

/// Creates (or fixes) a world-accessible character device node, like
/// `nvidia-modprobe` with the driver's default `NVreg_DeviceFileMode`.
fn char_device(path: &str, major: u32, minor: u32) -> Result<(), String> {
	let dev = libc::makedev(major, minor);
	if let Ok(meta) = fs::symlink_metadata(path) {
		if meta.file_type().is_char_device() && meta.rdev() == dev {
			return set_mode(path);
		}
		fs::remove_file(path).map_err(|err| format!("remove stale {path}: {err}"))?;
	}
	let c_path = CString::new(path).map_err(|err| err.to_string())?;
	// SAFETY: `c_path` is nul-terminated and outlives the call.
	if unsafe { libc::mknod(c_path.as_ptr(), libc::S_IFCHR | 0o666, dev) } != 0 {
		return Err(format!("mknod {path}: {}", io::Error::last_os_error()));
	}
	set_mode(path)
}

/// 0666 regardless of the agent's umask.
fn set_mode(path: &str) -> Result<(), String> {
	fs::set_permissions(path, fs::Permissions::from_mode(0o666))
		.map_err(|err| format!("chmod {path}: {err}"))
}

/// Adds the bundle's `lib/` to the dynamic linker search path and refreshes
/// the cache. Images without `ldconfig` (musl) cannot run the glibc driver
/// libraries anyway.
fn register_libraries(lib: &Path) -> Result<(), String> {
	let ldconfig = LDCONFIG
		.iter()
		.find(|path| Path::new(path).exists())
		.ok_or("no ldconfig in the rootfs (the driver libraries need glibc)")?;
	let entry = format!("{}\n", lib.display());
	if fs::read_to_string(LD_CONF).ok().as_deref() != Some(entry.as_str()) {
		fs::create_dir_all("/etc/ld.so.conf.d")
			.and_then(|()| fs::write(LD_CONF, &entry))
			.map_err(|err| format!("write {LD_CONF}: {err}"))?;
	}
	let output = Command::new(ldconfig)
		.output()
		.map_err(|err| format!("run {ldconfig}: {err}"))?;
	if output.status.success() {
		Ok(())
	} else {
		Err(format!(
			"{ldconfig} failed ({}): {}",
			output.status,
			String::from_utf8_lossy(&output.stderr).trim()
		))
	}
}

/// Symlinks every bundle tool into [`TOOL_DIR`], leaving tools the image
/// already ships alone.
fn link_tools(bin: &Path) {
	let Ok(entries) = fs::read_dir(bin) else {
		return;
	};
	for entry in entries.flatten() {
		let target = entry.path();
		let link = Path::new(TOOL_DIR).join(entry.file_name());
		match fs::read_link(&link) {
			Ok(current) if current == target => continue,
			Ok(current) if current.starts_with(MOUNT_POINT) => {
				let _ = fs::remove_file(&link);
			},
			Ok(_) => continue,
			Err(_) if link.symlink_metadata().is_ok() => continue,
			Err(_) => {},
		}
		if let Err(err) = symlink(&target, &link) {
			logging::emit(
				Level::Warn,
				"NVIDIA tool not linked",
				json!({"path": link.display().to_string(), "error": err.to_string()}),
			);
		}
	}
}

/// Unloads the NVIDIA driver so the GPU's GSP firmware shuts down cleanly
/// before the VM goes away.
///
/// A VM that exits with the driver loaded leaves GSP
/// running, VFIO's function-level reset does not stop it, and the next VM's
/// driver cannot boot the GPU ("WPR2 already up"). SIGKILLs every process
/// holding a `/dev/nvidia*` file (open or mapped) and waits for them to exit
/// first. Ok when the driver is not loaded.
///
/// # Errors
/// The module that could not be unloaded and why.
pub fn release() -> Result<(), String> {
	if !Path::new("/sys/module/nvidia").exists() {
		return Ok(());
	}
	let started = Instant::now();
	let deadline = started + UNLOAD_WAIT;
	let mut killed = Vec::new();
	loop {
		let users = gpu_users();
		if users.is_empty() {
			break;
		}
		if Instant::now() >= deadline {
			return Err(format!("processes {users:?} still hold /dev/nvidia* after SIGKILL"));
		}
		for &pid in &users {
			// SAFETY: plain signal delivery; no memory is passed.
			unsafe { libc::kill(pid, libc::SIGKILL) };
			if !killed.contains(&pid) {
				killed.push(pid);
			}
		}
		thread::sleep(Duration::from_millis(20));
	}
	for module in UNLOAD {
		unload_module(module, deadline).map_err(|err| format!("unload {module}: {err}"))?;
	}
	logging::emit(
		Level::Info,
		"NVIDIA GPU driver unloaded",
		json!({"killed": killed, "elapsed_ms": started.elapsed().as_millis()}),
	);
	Ok(())
}

/// Processes (other than this one) with a `/dev/nvidia*` file open or mapped.
/// Zombies have released their files and no longer show up.
fn gpu_users() -> Vec<i32> {
	// SAFETY: `getpid` has no preconditions.
	let me = unsafe { libc::getpid() };
	let Ok(entries) = fs::read_dir("/proc") else {
		return Vec::new();
	};
	entries
		.flatten()
		.filter_map(|entry| entry.file_name().to_str()?.parse::<i32>().ok())
		.filter(|&pid| pid != me && holds_gpu(pid))
		.collect()
}

fn holds_gpu(pid: i32) -> bool {
	let proc_dir = Path::new("/proc").join(pid.to_string());
	let open = fs::read_dir(proc_dir.join("fd")).is_ok_and(|fds| {
		fds.flatten().any(|fd| {
			fs::read_link(fd.path()).is_ok_and(|target| {
				target
					.as_os_str()
					.as_bytes()
					.starts_with(DEVICE_PREFIX.as_bytes())
			})
		})
	});
	open
		|| fs::read_to_string(proc_dir.join("maps"))
			.is_ok_and(|maps| maps.lines().any(|line| line.contains(DEVICE_PREFIX)))
}

/// `delete_module(2)`, retrying while killed processes still hold the module.
fn unload_module(name: &str, deadline: Instant) -> Result<(), String> {
	if !Path::new("/sys/module").join(name).exists() {
		return Ok(());
	}
	let c_name = CString::new(name).map_err(|err| err.to_string())?;
	loop {
		// SAFETY: `c_name` is nul-terminated and outlives the call; O_NONBLOCK
		// fails with EWOULDBLOCK instead of waiting while the module is in use.
		let rc = unsafe { libc::syscall(libc::SYS_delete_module, c_name.as_ptr(), libc::O_NONBLOCK) };
		if rc == 0 {
			return Ok(());
		}
		let err = io::Error::last_os_error();
		match err.raw_os_error() {
			Some(libc::ENOENT) => return Ok(()),
			Some(libc::EWOULDBLOCK | libc::EBUSY) if Instant::now() < deadline => {
				thread::sleep(Duration::from_millis(50));
			},
			_ => return Err(err.to_string()),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn manifest_parses() {
		let manifest: Manifest =
			serde_json::from_str(r#"{"driver": "615.71.09", "kernel": "6.16.9-vmon"}"#).unwrap();
		assert_eq!(manifest.driver, "615.71.09");
		assert_eq!(manifest.kernel, "6.16.9-vmon");
	}
}
