//! `vmon vmm` argument vector for a [`VmSpec`] (see the VMM CLI in
//! `vmm/src/config.rs`). Pure so the flag contract is unit-tested.

use std::{ffi::OsString, path::Path};

use super::{Boot, Disk, FsShare, VmGpu, VmNet, VmSpec, VmmIsolation};

/// Socket paths of one VM instance.
pub(super) struct VmSockets<'a> {
	pub api:   &'a Path,
	pub agent: &'a Path,
}

/// Builds the arguments that follow `vmon vmm`.
pub(super) fn vmm_argv(
	spec: &VmSpec,
	sockets: &VmSockets<'_>,
	isolation: VmmIsolation,
) -> Vec<OsString> {
	let mut args: Vec<OsString> = Vec::new();
	let mut flag = |name: &str, value: Option<OsString>| {
		args.push(name.into());
		if let Some(value) = value {
			args.push(value);
		}
	};
	match &spec.boot {
		Boot::Cold { kernel, initrd, cmdline } => {
			flag("--kernel", Some(kernel.into()));
			if let Some(initrd) = initrd {
				flag("--initrd", Some(initrd.into()));
			}
			flag("--cmdline", Some(cmdline.into()));
			flag("--cpus", Some(spec.vcpus.to_string().into()));
			flag("--mem", Some(spec.memory_mb.to_string().into()));
		},
		// CPU count and memory size come from the snapshot.
		Boot::Fork { snapshot } => flag("--fork-from", Some(snapshot.into())),
		// The page token travels in the environment, not argv.
		Boot::RemotePages { snapshot, pages } => {
			flag("--restore", Some(snapshot.into()));
			flag("--remote-page-url", Some(pages.url.clone().into()));
		},
	}
	match &spec.disk {
		Some(Disk::Direct { path, read_only }) => {
			flag("--rootfs", Some(path.into()));
			if *read_only {
				flag("--rootfs-ro", None);
			}
		},
		Some(Disk::Overlay { base, path }) => {
			flag("--disk-overlay-of", Some(base.into()));
			flag("--rootfs", Some(path.into()));
		},
		Some(Disk::Remote(remote)) => {
			flag("--rootfs", Some(remote.overlay.clone().into()));
			flag("--rootfs-remote-url", Some(remote.url.clone().into()));
			flag("--rootfs-remote-cache", Some(remote.cache.clone().into()));
			flag("--rootfs-remote-index", Some(remote.index.clone().into()));
			flag("--rootfs-remote-size", Some(remote.size.to_string().into()));
			flag("--rootfs-remote-etag", Some(remote.etag.clone().into()));
		},
		None => {},
	}
	flag("--api-sock", Some(sockets.api.into()));
	flag("--agent-sock", Some(sockets.agent.into()));
	flag("--rng", None);
	if let Some(root) = &spec.snapshot_root {
		flag("--snapshot-root", Some(root.into()));
	}
	for share in &spec.shares {
		match share {
			FsShare::ReadOnly { tag, dir } => {
				flag("--fs-tag", Some(tag.into()));
				flag("--fs-dir", Some(dir.into()));
			},
			FsShare::Volume { tag, dir, read_only } => {
				let mut value = OsString::from(format!("{tag}:"));
				value.push(dir);
				if *read_only {
					value.push(":ro");
				}
				flag("--volume", Some(value));
			},
		}
	}
	match &spec.net {
		VmNet::None => {},
		VmNet::User => flag("--net", Some("user".into())),
		VmNet::Tap { name, mac } => {
			flag("--tap", Some(name.into()));
			flag("--mac", Some(mac.into()));
		},
	}
	// The VMM preopens the functions and iommufd before it drops to the
	// sandbox uid, and binds them into a `--jail`.
	if let Some(VmGpu { functions, vm_uuid }) = &spec.gpu {
		flag("--boot-mode", Some("direct".into()));
		for function in functions {
			flag("--vfio-gpu", Some(function.into()));
		}
		flag("--vm-uuid", Some(vm_uuid.into()));
	}
	if let Some(timeout) = spec.timeout {
		flag("--timeout-secs", Some(timeout.as_secs().clamp(1, 86_400).to_string().into()));
	}
	match isolation {
		VmmIsolation::Default => {},
		VmmIsolation::DropTo { uid, gid } => {
			flag("--sandbox-uid", Some(uid.to_string().into()));
			flag("--sandbox-gid", Some(gid.to_string().into()));
		},
		VmmIsolation::Disabled => flag("--no-sandbox", None),
	}
	flag("--log-format", Some("json".into()));
	args.extend(spec.extra_args.iter().map(OsString::from));
	args
}

#[cfg(test)]
mod tests {
	use std::{path::PathBuf, time::Duration};

	use super::*;

	fn spec(boot: Boot) -> VmSpec {
		VmSpec {
			name: "sbx".into(),
			boot,
			vcpus: 2,
			memory_mb: 512,
			disk: Some(Disk::Overlay { base: "/b/rootfs.ext4".into(), path: "/s/rootfs.img".into() }),
			shares: vec![
				FsShare::ReadOnly { tag: "ctx".into(), dir: "/ctx".into() },
				FsShare::Volume {
					tag:       "v0".into(),
					dir:       "/vol/a".into(),
					read_only: false,
				},
				FsShare::Volume { tag: "v1".into(), dir: "/vol/b".into(), read_only: true },
			],
			net: VmNet::Tap { name: "vmon3".into(), mac: "02:76:6d:00:00:03".into() },
			snapshot_root: Some(PathBuf::from("/snap")),
			console_log: "/s/console.log".into(),
			timeout: Some(Duration::from_secs(90)),
			ready_timeout: Duration::from_secs(10),
			extra_args: vec!["--user-net-policy".into(), "/s/policy.json".into()],
			gpu: None,
		}
	}

	fn render(spec: &VmSpec, isolation: VmmIsolation) -> Vec<String> {
		let sockets =
			VmSockets { api: Path::new("/r/api.sock"), agent: Path::new("/r/agent.sock") };
		vmm_argv(spec, &sockets, isolation)
			.into_iter()
			.map(|a| a.into_string().unwrap())
			.collect()
	}

	fn value_of<'a>(args: &'a [String], flag: &str) -> Vec<&'a str> {
		args
			.windows(2)
			.filter(|w| w[0] == flag)
			.map(|w| w[1].as_str())
			.collect()
	}

	#[test]
	fn fork_uses_snapshot_and_overlay_and_never_sizes_the_vm() {
		let args =
			render(&spec(Boot::Fork { snapshot: "/b/snapshot".into() }), VmmIsolation::Default);
		assert_eq!(value_of(&args, "--fork-from"), ["/b/snapshot"]);
		assert_eq!(value_of(&args, "--disk-overlay-of"), ["/b/rootfs.ext4"]);
		assert_eq!(value_of(&args, "--rootfs"), ["/s/rootfs.img"]);
		// The VMM rejects/ignores sizing and kernels on fork; never emit them.
		for absent in ["--kernel", "--cmdline", "--cpus", "--mem", "--sandbox-uid", "--no-sandbox"] {
			assert!(!args.iter().any(|a| a == absent), "{absent} must not be passed on fork");
		}
		assert!(args.iter().any(|a| a == "--rng"));
		assert_eq!(value_of(&args, "--api-sock"), ["/r/api.sock"]);
		assert_eq!(value_of(&args, "--agent-sock"), ["/r/agent.sock"]);
	}

	#[test]
	fn cold_boot_passes_kernel_cmdline_sizing_and_shares() {
		let mut cold = spec(Boot::Cold {
			kernel:  "/k/Image".into(),
			initrd:  Some("/k/initrd".into()),
			cmdline: "console=ttyS0 init=/usr/bin/envd".into(),
		});
		cold.disk = Some(Disk::Direct { path: "/b/rootfs.ext4".into(), read_only: true });
		let args = render(&cold, VmmIsolation::DropTo { uid: 1000, gid: 1001 });
		assert_eq!(value_of(&args, "--kernel"), ["/k/Image"]);
		assert_eq!(value_of(&args, "--initrd"), ["/k/initrd"]);
		assert_eq!(value_of(&args, "--cmdline"), ["console=ttyS0 init=/usr/bin/envd"]);
		assert_eq!(value_of(&args, "--cpus"), ["2"]);
		assert_eq!(value_of(&args, "--mem"), ["512"]);
		assert!(args.iter().any(|a| a == "--rootfs-ro"));
		assert!(!args.iter().any(|a| a == "--disk-overlay-of"));
		assert_eq!(value_of(&args, "--volume"), ["v0:/vol/a", "v1:/vol/b:ro"]);
		assert_eq!(value_of(&args, "--fs-tag"), ["ctx"]);
		assert_eq!(value_of(&args, "--tap"), ["vmon3"]);
		assert_eq!(value_of(&args, "--mac"), ["02:76:6d:00:00:03"]);
		assert_eq!(value_of(&args, "--timeout-secs"), ["90"]);
		assert_eq!(value_of(&args, "--sandbox-uid"), ["1000"]);
		assert_eq!(value_of(&args, "--sandbox-gid"), ["1001"]);
		// Extra (egress) flags come last, verbatim.
		assert_eq!(&args[args.len() - 2..], ["--user-net-policy", "/s/policy.json"]);
	}

	#[test]
	fn lazy_resume_renders_remote_disk_and_remote_pages_without_secrets_in_argv() {
		let mut lazy = spec(Boot::RemotePages {
			snapshot: "/p/snapshot".into(),
			pages:    super::super::RemotePages {
				url:   "http://127.0.0.1:4100/pages".into(),
				token: "page-secret".into(),
			},
		});
		lazy.disk = Some(Disk::Remote(super::super::RemoteDisk {
			overlay: "/s/rootfs.img".into(),
			cache:   "/s/rootfs.cache".into(),
			index:   "/s/rootfs.blk.json".into(),
			url:     "http://s3/b/rootfs.blk?X-Amz-Signature=abc".into(),
			etag:    "\"e-1\"".into(),
			size:    1 << 30,
		}));
		let args = render(&lazy, VmmIsolation::Default);
		assert_eq!(value_of(&args, "--restore"), ["/p/snapshot"]);
		assert_eq!(value_of(&args, "--remote-page-url"), ["http://127.0.0.1:4100/pages"]);
		assert!(!args.iter().any(|a| a.contains("page-secret")));
		assert!(
			!args
				.iter()
				.any(|a| a == "--fork-from" || a == "--disk-overlay-of")
		);
		assert_eq!(value_of(&args, "--rootfs"), ["/s/rootfs.img"]);
		assert_eq!(value_of(&args, "--rootfs-remote-cache"), ["/s/rootfs.cache"]);
		assert_eq!(value_of(&args, "--rootfs-remote-index"), ["/s/rootfs.blk.json"]);
		assert_eq!(value_of(&args, "--rootfs-remote-size"), ["1073741824"]);
		assert_eq!(value_of(&args, "--rootfs-remote-etag"), ["\"e-1\""]);
		assert_eq!(value_of(&args, "--rootfs-remote-url"), [
			"http://s3/b/rootfs.blk?X-Amz-Signature=abc"
		]);
		let debug = format!("{lazy:?}");
		assert!(!debug.contains("page-secret") && !debug.contains("X-Amz-Signature"));
	}

	#[test]
	fn gpu_passthrough_renders_functions_uuid_and_direct_boot() {
		let mut cold = spec(Boot::Cold {
			kernel:  "/k/bzImage".into(),
			initrd:  None,
			cmdline: "console=ttyS0".into(),
		});
		let args = render(&cold, VmmIsolation::Disabled);
		assert!(!args.iter().any(|a| a == "--vfio-gpu" || a == "--vm-uuid"));
		cold.gpu = Some(VmGpu {
			functions: vec![
				"/sys/bus/pci/devices/0000:52:00.0".into(),
				"/sys/bus/pci/devices/0000:53:00.0".into(),
			],
			vm_uuid:   "12345678-9abc-8ef0-9122-334455667788".into(),
		});
		let args = render(&cold, VmmIsolation::DropTo { uid: 1000, gid: 1000 });
		assert_eq!(value_of(&args, "--vfio-gpu"), [
			"/sys/bus/pci/devices/0000:52:00.0",
			"/sys/bus/pci/devices/0000:53:00.0"
		]);
		assert_eq!(value_of(&args, "--vm-uuid"), ["12345678-9abc-8ef0-9122-334455667788"]);
		assert_eq!(value_of(&args, "--boot-mode"), ["direct"]);
		assert_eq!(value_of(&args, "--sandbox-uid"), ["1000"]);
	}

	#[test]
	fn user_net_and_disabled_isolation_and_timeout_clamp() {
		let mut user = spec(Boot::Fork { snapshot: "/b/snapshot".into() });
		user.net = VmNet::User;
		user.timeout = Some(Duration::from_hours(240));
		user.snapshot_root = None;
		let args = render(&user, VmmIsolation::Disabled);
		assert_eq!(value_of(&args, "--net"), ["user"]);
		assert!(
			!args
				.iter()
				.any(|a| a == "--tap" || a == "--mac" || a == "--snapshot-root")
		);
		assert_eq!(value_of(&args, "--timeout-secs"), ["86400"]);
		assert!(args.iter().any(|a| a == "--no-sandbox"));
	}
}
