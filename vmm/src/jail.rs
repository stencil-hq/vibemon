//! Linux two-process jailer for production VM isolation.
//!
//! The original vmon process remains the jailer parent. It creates the jail
//! filesystem/cgroup, unshares the VM namespaces, forks a child that becomes
//! PID 1 in the new PID namespace, and exits with the child's status. The child
//! pivots into the jail, pre-binds operator-owned sockets as root, and then
//! enters the normal VMM lifecycle. Stage-B filters/drop happen after backends
//! open.

#[cfg(target_arch = "x86_64")]
use crate::devices::vfio::gpu_cdev_path;
use crate::{config::Config, result::Result};
pub fn fork_into_jail(config: &Config) -> Result<()> {
	require_root()?;
	let launch_uid = launch_owner_uid()?;
	let id = config
		.id
		.as_deref()
		.ok_or_else(|| err("--jail requires --id"))?;
	validate_id(id)?;
	let jail_root = jail_root(config, id);
	let chroot = jail_root.join("root");

	prepare_jail_tree(&jail_root, &chroot)?;
	let cgroup = Cgroup::setup(config, id)?;
	let cleanup_paths = jail_socket_cleanup_paths(config, &chroot);

	unshare_jail_namespaces()?;
	make_mounts_private()?;
	make_jail_root_mount(&chroot)?;
	bind_configured_paths(config, &chroot)?;

	// SAFETY: no Rust locks are held across fork; both parent and child immediately
	// follow simple, checked control-flow paths.
	let pid = unsafe { libc::fork() };
	if pid < 0 {
		return Err(os_error("fork jail child"));
	}
	if pid > 0 {
		if let Err(e) = cgroup.place(pid) {
			// SAFETY: the child pid came from `fork`; best-effort kill/reap is confined
			// to that child after cgroup placement failed.
			unsafe {
				libc::kill(pid, libc::SIGKILL);
				let mut status = 0;
				libc::waitpid(pid, &mut status, 0);
			}
			return Err(e);
		}
		wait_for_child_and_exit(pid, &cleanup_paths);
	}

	let mut child_config = config.clone();
	child_main(&mut child_config, launch_uid, &chroot)
}
use std::{
	ffi::CString,
	fs::{self, File},
	io,
	os::{
		fd::AsRawFd,
		unix::{
			ffi::OsStrExt,
			fs::{DirBuilderExt, FileTypeExt, PermissionsExt},
		},
	},
	path::{Component, Path, PathBuf},
	process,
};

use tracing::error;

use crate::{config::CgroupMode, result::err};
const DEFAULT_PIDS_MAX: u64 = 1024;
const JAIL_RUN_DIR: &str = "/run/vmon";
const DEFAULT_CONTROL_SOCK: &str = "/run/vmon/control.sock";

fn child_main(config: &mut Config, launch_uid: u32, chroot: &Path) -> Result<()> {
	mount_proc(chroot)?;
	pivot_into(chroot)?;
	configure_jail_socket_paths(config)?;
	if let Some(netns) = &config.netns {
		setns_net(netns)?;
	}
	let (uid, gid) = sandbox_identity(config)?;
	config.sandbox_uid = Some(uid);
	config.sandbox_gid = Some(gid);

	let prebound = crate::vmm::prebind_jail_sockets(config, launch_uid)?;
	crate::vmm::run_inner_with_prebound(config.clone(), prebound)
}

fn require_root() -> Result<()> {
	// SAFETY: `geteuid` has no preconditions and only reads process credentials.
	if unsafe { libc::geteuid() } != 0 {
		return Err(err("--jail requires launching vmon as root"));
	}
	Ok(())
}

fn launch_owner_uid() -> Result<u32> {
	// SAFETY: `getuid` has no preconditions and only reads process credentials.
	let real_uid = unsafe { libc::getuid() as u32 };
	if real_uid != 0 {
		return Ok(real_uid);
	}
	Ok(parse_env_u32("SUDO_UID")?
		.filter(|uid| *uid != 0)
		.unwrap_or(0))
}

fn validate_id(id: &str) -> Result<()> {
	if id.is_empty()
		|| id.len() > 64
		|| !id
			.bytes()
			.all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
	{
		return Err(err("--id must be 1..=64 ASCII letters, digits, '-' or '_'"));
	}
	Ok(())
}

fn jail_root(config: &Config, id: &str) -> PathBuf {
	config
		.jail_root
		.clone()
		.unwrap_or_else(|| PathBuf::from("/srv/jailer").join(id))
}

fn prepare_jail_tree(jail_root: &Path, chroot: &Path) -> Result<()> {
	create_dir(jail_root, 0o700)?;
	chown_root(jail_root)?;
	chmod(jail_root, 0o700)?;

	create_dir(chroot, 0o755)?;
	chown_root(chroot)?;
	chmod(chroot, 0o755)?;

	for dir in ["run", "dev", "dev/net", "proc"] {
		let path = chroot.join(dir);
		create_dir(&path, 0o755)?;
		chown_root(&path)?;
	}

	let runtime = chroot.join("run/vmon");
	create_dir(&runtime, 0o700)?;
	chown_root(&runtime)?;
	chmod(&runtime, 0o700)?;
	Ok(())
}

fn create_dir(path: &Path, mode: u32) -> Result<()> {
	fs::DirBuilder::new()
		.recursive(true)
		.mode(mode)
		.create(path)
		.map_err(|e| err(format!("creating {}: {e}", path.display())))
}

fn chmod(path: &Path, mode: u32) -> Result<()> {
	fs::set_permissions(path, fs::Permissions::from_mode(mode))
		.map_err(|e| err(format!("chmoding {} to {mode:o}: {e}", path.display())))
}

fn chown_root(path: &Path) -> Result<()> {
	let c_path = cstring_path(path)?;
	// SAFETY: `c_path` is a valid C string; uid/gid are scalar root ids.
	let rc = unsafe { libc::chown(c_path.as_ptr(), 0, 0) };
	if rc != 0 {
		return Err(os_error_path("chown root:root", path));
	}
	Ok(())
}

struct Cgroup {
	path: Option<PathBuf>,
}

impl Cgroup {
	fn setup(config: &Config, id: &str) -> Result<Self> {
		match config.cgroup_mode {
			CgroupMode::Off => Ok(Self { path: None }),
			CgroupMode::V2 => Self::setup_v2(config, id),
		}
	}

	fn setup_v2(config: &Config, id: &str) -> Result<Self> {
		let root = Path::new("/sys/fs/cgroup");
		if !root.join("cgroup.controllers").exists() {
			return Err(err(
				"cgroup v2 is not mounted at /sys/fs/cgroup; use --cgroup-mode off to disable cgroup \
				 setup",
			));
		}
		// cgroup v2 exposes a controller's files in a child only when the
		// parent enables it in cgroup.subtree_control; a freshly created
		// /sys/fs/cgroup/vmon starts with none enabled, so enable exactly the
		// controllers whose limits are written below. Idempotent when already
		// enabled; fails clearly when the host has not delegated one of them.
		let parent = root.join("vmon");
		create_dir(&parent, 0o755)?;
		write_value(&parent.join("cgroup.subtree_control"), "+cpu +memory +pids").map_err(|e| {
			err(format!(
				"enabling cpu/memory/pids controllers under {}: {e} (check {}/cgroup.subtree_control)",
				parent.display(),
				root.display()
			))
		})?;
		let path = parent.join(id);
		create_dir(&path, 0o755)?;
		write_value(&path.join("cpu.max"), config.cgroup_cpu_max.as_deref().unwrap_or("max"))?;
		let memory_max = config
			.cgroup_mem_max
			.clone()
			.unwrap_or_else(|| default_memory_max(config.mem_mib));
		write_value(&path.join("memory.max"), &memory_max)?;
		let pids_max = config
			.cgroup_pids_max
			.unwrap_or(DEFAULT_PIDS_MAX)
			.to_string();
		write_value(&path.join("pids.max"), &pids_max)?;
		Ok(Self { path: Some(path) })
	}

	fn place(&self, pid: libc::pid_t) -> Result<()> {
		if let Some(path) = &self.path {
			write_value(&path.join("cgroup.procs"), &pid.to_string())?;
		}
		Ok(())
	}
}

fn default_memory_max(mem_mib: usize) -> String {
	let mib = 1024_u128 * 1024;
	let bytes = (mem_mib as u128).saturating_mul(5).saturating_mul(mib) / 4;
	bytes.to_string()
}

fn write_value(path: &Path, value: &str) -> Result<()> {
	fs::write(path, value).map_err(|e| err(format!("writing {}: {e}", path.display())))
}

fn unshare_jail_namespaces() -> Result<()> {
	let flags = libc::CLONE_NEWNS | libc::CLONE_NEWPID | libc::CLONE_NEWIPC | libc::CLONE_NEWUTS;
	// SAFETY: `unshare` takes scalar clone flags and returns synchronously.
	let rc = unsafe { libc::unshare(flags) };
	if rc != 0 {
		return Err(os_error("unshare(CLONE_NEWNS|CLONE_NEWPID|CLONE_NEWIPC|CLONE_NEWUTS)"));
	}
	Ok(())
}

fn make_mounts_private() -> Result<()> {
	let target = CString::new("/").expect("literal has no NUL");
	// SAFETY: the target path is a valid C string for the duration of the call;
	// null source/fstype/data are the documented propagation-change form.
	let rc = unsafe {
		libc::mount(
			std::ptr::null(),
			target.as_ptr(),
			std::ptr::null(),
			(libc::MS_REC | libc::MS_PRIVATE) as libc::c_ulong,
			std::ptr::null(),
		)
	};
	if rc != 0 {
		return Err(os_error("mount MS_PRIVATE /"));
	}
	Ok(())
}

fn make_jail_root_mount(chroot: &Path) -> Result<()> {
	mount_bind(chroot, chroot, false)
}

fn bind_configured_paths(config: &Config, chroot: &Path) -> Result<()> {
	bind_optional(chroot, config.kernel.as_deref(), true)?;
	bind_optional(chroot, config.initrd.as_deref(), true)?;
	bind_optional(chroot, config.firmware.as_deref(), true)?;
	bind_optional(chroot, config.restore.as_deref(), true)?;
	bind_optional(chroot, config.fork_from.as_deref(), true)?;
	bind_optional(chroot, config.zram_swap_file.as_deref(), false)?;
	bind_optional(chroot, config.disk_overlay_of.as_deref(), true)?;
	bind_optional(chroot, config.fs_dir.as_deref(), true)?;
	for m in &config.volumes {
		bind_path(chroot, &m.dir, m.read_only)?;
	}
	for m in &config.remote_fs {
		bind_remote_socket(chroot, &m.sock)?;
	}
	bind_optional(chroot, config.netns.as_deref(), true)?;
	#[cfg(target_arch = "x86_64")]
	if !config.vfio_gpus.is_empty() {
		bind_path(chroot, Path::new("/dev/iommu"), false)?;
		for gpu in &config.vfio_gpus {
			bind_path(chroot, gpu, true)?;
			bind_path(chroot, &gpu_cdev_path(gpu)?, false)?;
		}
	}

	if let Some(snapshot_root) = &config.snapshot_root {
		create_dir(snapshot_root, 0o700)?;
		bind_path(chroot, snapshot_root, false)?;
	}
	if let Some(cache) = &config.rootfs_remote_cache {
		if cache.exists() {
			bind_path(chroot, cache, false)?;
		} else {
			ensure_target_parent(chroot, cache)?;
		}
	}
	bind_optional(chroot, config.rootfs_remote_index.as_deref(), true)?;

	if let Some(rootfs) = &config.rootfs {
		if (config.disk_overlay_of.is_some() || config.rootfs_remote_url.is_some())
			&& !rootfs.exists()
		{
			ensure_target_parent(chroot, rootfs)?;
		} else {
			bind_path(chroot, rootfs, config.rootfs_read_only)?;
		}
	}

	for dev in ["/dev/kvm", "/dev/null", "/dev/urandom", "/dev/random"] {
		bind_path(chroot, Path::new(dev), false)?;
	}
	let tun = Path::new("/dev/net/tun");
	if tun.exists() {
		bind_path(chroot, tun, false)?;
	} else if config.tap.is_some() {
		return Err(err("--tap requires /dev/net/tun in the jail"));
	}
	Ok(())
}

fn bind_optional(chroot: &Path, path: Option<&Path>, readonly: bool) -> Result<()> {
	let Some(path) = path else {
		return Ok(());
	};
	bind_path(chroot, path, readonly)
}

fn bind_path(chroot: &Path, source: &Path, readonly: bool) -> Result<()> {
	if !source.is_absolute() {
		return Err(err(format!("jail bind source {} must be an absolute path", source.display())));
	}
	let meta = fs::symlink_metadata(source)
		.map_err(|e| err(format!("checking jail bind source {}: {e}", source.display())))?;
	if meta.file_type().is_symlink() {
		return Err(err(format!("jail bind source {} must not be a symlink", source.display())));
	}
	let target = path_in_chroot(chroot, source)?;
	if meta.is_dir() {
		create_dir(&target, 0o755)?;
	} else {
		ensure_file_mountpoint(&target)?;
	}
	mount_bind(source, &target, readonly)
}

/// Bind a proxy socket, falling back to its parent directory on kernels that
/// reject bind-mounting a Unix-domain socket inode.
fn bind_remote_socket(chroot: &Path, sock: &Path) -> Result<()> {
	match bind_path(chroot, sock, false) {
		Ok(()) => Ok(()),
		Err(sock_error) => {
			let parent = sock.parent().ok_or_else(|| {
				err(format!("remote filesystem socket {} must have a parent directory", sock.display()))
			})?;
			bind_path(chroot, parent, false).map_err(|parent_error| {
				err(format!(
					"binding remote filesystem socket {} failed ({sock_error}); parent-directory \
					 fallback failed ({parent_error})",
					sock.display()
				))
			})
		},
	}
}

fn ensure_target_parent(chroot: &Path, path: &Path) -> Result<()> {
	let target = path_in_chroot(chroot, path)?;
	let parent = target
		.parent()
		.ok_or_else(|| err(format!("jail path {} must have a parent directory", path.display())))?;
	create_dir(parent, 0o755)
}

fn ensure_file_mountpoint(target: &Path) -> Result<()> {
	if let Some(parent) = target.parent() {
		create_dir(parent, 0o755)?;
	}
	if !target.exists() {
		File::create(target)
			.map_err(|e| err(format!("creating bind mountpoint {}: {e}", target.display())))?;
	}
	Ok(())
}

fn path_in_chroot(chroot: &Path, absolute: &Path) -> Result<PathBuf> {
	let relative = absolute
		.strip_prefix("/")
		.map_err(|_| err(format!("jail path {} must be absolute", absolute.display())))?;
	if relative
		.components()
		.any(|component| !matches!(component, Component::Normal(_)))
	{
		return Err(err(format!(
			"jail path {} must not contain '.' or '..' components",
			absolute.display()
		)));
	}
	Ok(chroot.join(relative))
}

fn mount_bind(source: &Path, target: &Path, readonly: bool) -> Result<()> {
	let source_c = cstring_path(source)?;
	let target_c = cstring_path(target)?;
	let recursive = if source.is_dir() { libc::MS_REC } else { 0 };
	// SAFETY: source and target are valid C strings for the duration of the call;
	// null fstype/data are the documented bind-mount form.
	let rc = unsafe {
		libc::mount(
			source_c.as_ptr(),
			target_c.as_ptr(),
			std::ptr::null(),
			(libc::MS_BIND | recursive) as libc::c_ulong,
			std::ptr::null(),
		)
	};
	if rc != 0 {
		return Err(err(format!(
			"bind-mounting {} to {}: {}",
			source.display(),
			target.display(),
			io::Error::last_os_error()
		)));
	}
	if readonly {
		remount_readonly(target)?;
	}
	Ok(())
}

fn remount_readonly(target: &Path) -> Result<()> {
	let target_c = cstring_path(target)?;
	let recursive = if target.is_dir() { libc::MS_REC } else { 0 };
	// SAFETY: the target path is a valid C string for the duration of the call;
	// null source/fstype/data are the documented remount form.
	let rc = unsafe {
		libc::mount(
			std::ptr::null(),
			target_c.as_ptr(),
			std::ptr::null(),
			(libc::MS_BIND | libc::MS_REMOUNT | libc::MS_RDONLY | recursive) as libc::c_ulong,
			std::ptr::null(),
		)
	};
	if rc != 0 {
		return Err(os_error_path("remount read-only", target));
	}
	Ok(())
}

fn mount_proc(chroot: &Path) -> Result<()> {
	let target = chroot.join("proc");
	create_dir(&target, 0o755)?;
	let source = CString::new("proc").expect("literal has no NUL");
	let fstype = CString::new("proc").expect("literal has no NUL");
	let target_c = cstring_path(&target)?;
	// SAFETY: source, fstype, and target are valid C strings for the duration of
	// the call.
	let rc = unsafe {
		libc::mount(source.as_ptr(), target_c.as_ptr(), fstype.as_ptr(), 0, std::ptr::null())
	};
	if rc != 0 {
		return Err(os_error_path("mount proc", &target));
	}
	Ok(())
}

fn pivot_into(chroot: &Path) -> Result<()> {
	let put_old = chroot.join(".pivot_old");
	create_dir(&put_old, 0o700)?;
	let chroot_c = cstring_path(chroot)?;
	let put_old_c = cstring_path(&put_old)?;
	// SAFETY: both paths are valid C strings and refer to prepared mount points.
	let rc = unsafe { libc::syscall(libc::SYS_pivot_root, chroot_c.as_ptr(), put_old_c.as_ptr()) };
	if rc != 0 {
		return Err(os_error("pivot_root"));
	}
	let slash = CString::new("/").expect("literal has no NUL");
	// SAFETY: the slash C string is valid for the duration of the call.
	if unsafe { libc::chdir(slash.as_ptr()) } != 0 {
		return Err(os_error("chdir(/) after pivot_root"));
	}
	let old = CString::new("/.pivot_old").expect("literal has no NUL");
	// SAFETY: the old-root C string is valid and names the detached old root mount.
	if unsafe { libc::umount2(old.as_ptr(), libc::MNT_DETACH) } != 0 {
		return Err(os_error("umount old root"));
	}
	fs::remove_dir("/.pivot_old").map_err(|e| err(format!("removing old root: {e}")))?;
	Ok(())
}

fn configure_jail_socket_paths(config: &mut Config) -> Result<()> {
	if config.api_sock.is_none() {
		config.api_sock = Some(PathBuf::from(DEFAULT_CONTROL_SOCK));
	}
	if let Some(sock) = &config.api_sock {
		ensure_runtime_socket("--api-sock", sock)?;
	}
	if let Some(sock) = &config.agent_sock {
		ensure_runtime_socket("--agent-sock", sock)?;
	}
	Ok(())
}

fn ensure_runtime_socket(flag: &str, path: &Path) -> Result<()> {
	if !path.is_absolute()
		|| !path.starts_with(JAIL_RUN_DIR)
		|| path
			.components()
			.any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
	{
		return Err(err(format!(
			"{flag} under --jail must be inside {JAIL_RUN_DIR} (got {})",
			path.display()
		)));
	}
	Ok(())
}

fn setns_net(path: &Path) -> Result<()> {
	let file =
		File::open(path).map_err(|e| err(format!("opening --netns {}: {e}", path.display())))?;
	// SAFETY: `file` is an open namespace fd and `setns` does not retain it.
	let rc = unsafe { libc::setns(file.as_raw_fd(), libc::CLONE_NEWNET) };
	if rc != 0 {
		return Err(os_error_path("setns(CLONE_NEWNET)", path));
	}
	Ok(())
}

fn sandbox_identity(config: &Config) -> Result<(u32, u32)> {
	let uid = match config.sandbox_uid {
		Some(uid) => uid,
		None => parse_env_u32("VMON_SANDBOX_UID")?
			.ok_or_else(|| err("--jail requires --sandbox-uid or VMON_SANDBOX_UID"))?,
	};
	let gid = match config.sandbox_gid {
		Some(gid) => gid,
		None => parse_env_u32("VMON_SANDBOX_GID")?
			.ok_or_else(|| err("--jail requires --sandbox-gid or VMON_SANDBOX_GID"))?,
	};
	if uid == 0 || gid == 0 {
		return Err(err("--jail sandbox uid/gid must be non-zero"));
	}
	Ok((uid, gid))
}

fn parse_env_u32(name: &str) -> Result<Option<u32>> {
	match std::env::var(name) {
		Ok(value) => value
			.parse()
			.map(Some)
			.map_err(|_| err(format!("invalid value {value:?} for {name}"))),
		Err(std::env::VarError::NotPresent) => Ok(None),
		Err(std::env::VarError::NotUnicode(_)) => Err(err(format!("{name} must be valid UTF-8"))),
	}
}

fn jail_socket_cleanup_paths(config: &Config, chroot: &Path) -> Vec<PathBuf> {
	let mut paths = Vec::new();
	let control = config
		.api_sock
		.as_deref()
		.unwrap_or_else(|| Path::new(DEFAULT_CONTROL_SOCK));
	if let Some(path) = runtime_socket_cleanup_path(chroot, control) {
		paths.push(path);
	}
	if let Some(agent) = &config.agent_sock
		&& let Some(path) = runtime_socket_cleanup_path(chroot, agent)
	{
		paths.push(path);
	}
	paths
}

fn runtime_socket_cleanup_path(chroot: &Path, path: &Path) -> Option<PathBuf> {
	if !path.is_absolute()
		|| !path.starts_with(JAIL_RUN_DIR)
		|| path
			.components()
			.any(|component| !matches!(component, Component::RootDir | Component::Normal(_)))
	{
		return None;
	}
	Some(chroot.join(path.strip_prefix("/").ok()?))
}

fn cleanup_jail_runtime_sockets(paths: &[PathBuf]) {
	for path in paths {
		let Ok(meta) = fs::symlink_metadata(path) else {
			continue;
		};
		if meta.file_type().is_socket() {
			let _ = fs::remove_file(path);
		}
	}
}

fn wait_for_child_and_exit(pid: libc::pid_t, cleanup_paths: &[PathBuf]) -> ! {
	let mut status = 0;
	loop {
		// SAFETY: `pid` is the forked child and `status` points to initialized storage.
		let rc = unsafe { libc::waitpid(pid, &mut status, 0) };
		if rc == pid {
			break;
		}
		if rc < 0 {
			let e = io::Error::last_os_error();
			if e.raw_os_error() == Some(libc::EINTR) {
				continue;
			}
			error!("waiting for jail child {pid}: {e}");
			process::exit(1);
		}
	}
	cleanup_jail_runtime_sockets(cleanup_paths);
	if libc::WIFEXITED(status) {
		process::exit(libc::WEXITSTATUS(status));
	}
	if libc::WIFSIGNALED(status) {
		process::exit(128 + libc::WTERMSIG(status));
	}
	process::exit(1);
}

fn cstring_path(path: &Path) -> Result<CString> {
	CString::new(path.as_os_str().as_bytes())
		.map_err(|_| err(format!("path {} contains an interior NUL byte", path.display())))
}

fn os_error(context: &str) -> crate::result::Error {
	err(format!("{context}: {}", io::Error::last_os_error()))
}

fn os_error_path(context: &str, path: &Path) -> crate::result::Error {
	err(format!("{context} {}: {}", path.display(), io::Error::last_os_error()))
}
