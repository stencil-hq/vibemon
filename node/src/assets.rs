//! Guest boot assets.
//!
//! The guest kernel (the vmon kernel shipped next to the
//! binary or built into `target/kernel/`, else a pinned cloud-hypervisor
//! kernel downloaded and sha256-verified on first use) and the static
//! `vmon-agent` binary for the guest arch, and the guest NVIDIA driver bundle
//! of GPU sandboxes. Resolved once by `Node::start`.

use std::{
	fs,
	io::Read,
	path::{Path, PathBuf},
};

use anyhow::{Context, bail, ensure};
use sha2::{Digest, Sha256};

/// A pinned upstream guest kernel: the last-resort fallback when no vmon
/// kernel is available locally. It lacks netfilter, IPv6, bridge and NFS.
struct KernelPin {
	arch:   &'static str,
	name:   &'static str,
	url:    &'static str,
	sha256: &'static str,
}

const KERNELS: &[KernelPin] = &[
	KernelPin {
		arch: "aarch64",
		name: "Image-aarch64",
		url: "https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v6.16.9-20260508/Image-arm64",
		sha256: "69d1b1235381ec50f1b45cf771a7dff4a9013d452833ab34682d6283e2114010",
	},
	KernelPin {
		arch: "x86_64",
		name: "bzImage-x86_64",
		url: "https://github.com/cloud-hypervisor/linux/releases/download/ch-release-v6.12.8-20250613/bzImage-x86_64",
		sha256: "d4af401aa859e4659d4b08a153ac608eb6a315c6918e567daa46981af5d2e5ef",
	},
];

/// Guest architecture served by this host (`x86_64` | `aarch64`).
pub fn host_arch() -> &'static str {
	match std::env::consts::ARCH {
		"aarch64" => "aarch64",
		_ => "x86_64",
	}
}

fn sha256_file(path: &Path) -> anyhow::Result<String> {
	let mut file = fs::File::open(path)?;
	let mut hasher = Sha256::new();
	let mut buf = vec![0u8; 1 << 20];
	loop {
		let n = file.read(&mut buf)?;
		if n == 0 {
			break;
		}
		hasher.update(&buf[..n]);
	}
	Ok(hex::encode(hasher.finalize()))
}

/// Boot image name `kernel/build.sh` writes into `target/kernel/` for `arch`.
fn dev_kernel_name(arch: &str) -> &'static str {
	match arch {
		"aarch64" => "Image-aarch64",
		_ => "bzImage-x86_64",
	}
}

/// The guest kernel to use without downloading anything, in order:
/// `explicit` (`--kernel`, must exist), `vmon-kernel-<arch>` next to the
/// `vmon` binary (release assets), `target/kernel/<image>` in the nearest
/// ancestor of the binary's directory (dev tree after `just kernel`).
fn local_kernel(
	explicit: Option<&Path>,
	vmon_bin: &Path,
	arch: &str,
) -> anyhow::Result<Option<PathBuf>> {
	if let Some(path) = explicit {
		ensure!(path.is_file(), "guest kernel {} does not exist", path.display());
		return Ok(Some(path.to_owned()));
	}
	let exe_dir = vmon_bin.parent().unwrap_or_else(|| Path::new("."));
	let bundled = exe_dir.join(format!("vmon-kernel-{arch}"));
	if bundled.is_file() {
		return Ok(Some(bundled));
	}
	Ok(exe_dir
		.ancestors()
		.map(|dir| dir.join("target/kernel").join(dev_kernel_name(arch)))
		.find(|candidate| candidate.is_file()))
}

/// Returns the [`local_kernel`] if there is one, else
/// `<data_dir>/assets/<pinned kernel>`, downloading and verifying it when
/// missing or corrupt.
pub async fn resolve_kernel(
	explicit: Option<&Path>,
	vmon_bin: &Path,
	data_dir: &Path,
	http: &reqwest::Client,
) -> anyhow::Result<PathBuf> {
	let arch = host_arch();
	if let Some(path) = local_kernel(explicit, vmon_bin, arch)? {
		tracing::info!(kernel = %path.display(), "guest kernel");
		return Ok(path);
	}
	tracing::warn!(
		"no vmon guest kernel (vmon-kernel-{arch} next to vmon, or target/kernel/ from `just \
		 kernel`); falling back to the pinned cloud-hypervisor kernel, which lacks netfilter \
		 (nftables/iptables, Docker), IPv6, bridge and NFS"
	);
	let pin = KERNELS
		.iter()
		.find(|pin| pin.arch == arch)
		.context("no pinned guest kernel for this arch; pass --kernel")?;
	let dir = data_dir.join("assets");
	fs::create_dir_all(&dir)?;
	let dest = dir.join(pin.name);
	if dest.is_file() && sha256_file(&dest)? == pin.sha256 {
		return Ok(dest);
	}
	tracing::info!(url = pin.url, "downloading guest kernel (one-time)");
	let tmp = dir.join(format!("{}.part.{}", pin.name, std::process::id()));
	let result = async {
		let response = http.get(pin.url).send().await?.error_for_status()?;
		let bytes = response.bytes().await?;
		let got = hex::encode(Sha256::digest(&bytes));
		ensure!(
			got == pin.sha256,
			"checksum mismatch for {}: expected {}, got {got}",
			pin.url,
			pin.sha256
		);
		fs::write(&tmp, &bytes)?;
		fs::rename(&tmp, &dest)?;
		Ok(())
	}
	.await;
	let _ = fs::remove_file(&tmp);
	result.with_context(|| format!("downloading guest kernel from {}", pin.url))?;
	Ok(dest)
}

/// The guest NVIDIA driver bundle directory without downloading anything,
/// in the order of [`local_kernel`]: `explicit` (`--gpu-driver`, must be a
/// directory), `vmon-nvidia-<arch>/` next to the `vmon` binary (release
/// assets), `target/kernel/vmon-nvidia-<arch>/` in the nearest ancestor of
/// the binary's directory (dev tree after `just gpu-driver`).
fn local_gpu_driver(
	explicit: Option<&Path>,
	vmon_bin: &Path,
	arch: &str,
) -> anyhow::Result<Option<PathBuf>> {
	if let Some(path) = explicit {
		ensure!(path.is_dir(), "GPU driver bundle {} is not a directory", path.display());
		return Ok(Some(path.to_owned()));
	}
	let name = format!("vmon-nvidia-{arch}");
	let exe_dir = vmon_bin.parent().unwrap_or_else(|| Path::new("."));
	let bundled = exe_dir.join(&name);
	if bundled.is_dir() {
		return Ok(Some(bundled));
	}
	Ok(exe_dir
		.ancestors()
		.map(|dir| dir.join("target/kernel").join(&name))
		.find(|candidate| candidate.is_dir()))
}

/// Resolves the guest NVIDIA driver bundle ([`local_gpu_driver`]) together
/// with the release of the guest `kernel` its modules must match. A missing
/// bundle is not an error: GPUs then report it as their problem.
pub fn resolve_gpu_driver(
	explicit: Option<&Path>,
	vmon_bin: &Path,
	kernel: &Path,
) -> anyhow::Result<crate::gpu::DriverBundle> {
	let arch = host_arch();
	let dir = local_gpu_driver(explicit, vmon_bin, arch)?;
	let kernel = match &dir {
		Some(dir) => {
			let release = fs::read(kernel)
				.ok()
				.and_then(|image| crate::gpu::kernel_release(&image));
			tracing::info!(
				bundle = %dir.display(),
				kernel = release.as_deref().unwrap_or("unknown"),
				"guest GPU driver bundle"
			);
			release
		},
		None => None,
	};
	Ok(crate::gpu::DriverBundle { dir, kernel, searched: format!("vmon-nvidia-{arch}") })
}

/// Returns `explicit` if given, else `vmon-agent-<arch>` next to the `vmon`
/// binary, else the dev build at `<exe_dir>/../../target/test-assets/`.
/// The result must be a static ELF for the guest arch.
pub fn resolve_agent(explicit: Option<&Path>, vmon_bin: &Path) -> anyhow::Result<PathBuf> {
	let arch = host_arch();
	let path = if let Some(path) = explicit {
		path.to_owned()
	} else {
		let name = format!("vmon-agent-{arch}");
		let exe_dir = vmon_bin.parent().unwrap_or_else(|| Path::new("."));
		let candidates = [exe_dir.join(&name), exe_dir.join("../../target/test-assets").join(&name)];
		match candidates.iter().find(|candidate| candidate.is_file()) {
			Some(found) => found.clone(),
			None => bail!(
				"guest agent {name} not found next to {} or in target/test-assets; build it with \
				 `just agent-musl` or pass --agent",
				vmon_bin.display()
			),
		}
	};
	let header = {
		let mut file = fs::File::open(&path)
			.with_context(|| format!("opening guest agent {}", path.display()))?;
		let mut header = vec![0u8; 4096];
		let n = file.read(&mut header)?;
		header.truncate(n);
		header
	};
	check_static_elf(&header, arch).with_context(|| format!("guest agent {}", path.display()))?;
	Ok(path)
}

/// Verifies an ELF64 little-endian executable for `arch` without a
/// `PT_INTERP` (dynamic loader) segment. Needs the first program headers in
/// `header`.
fn check_static_elf(header: &[u8], arch: &str) -> anyhow::Result<()> {
	ensure!(header.len() >= 64 && header[..4] == *b"\x7fELF", "not an ELF file");
	ensure!(header[4] == 2 && header[5] == 1, "not a 64-bit little-endian ELF");
	let u16_at = |off: usize| u16::from_le_bytes([header[off], header[off + 1]]);
	let machine = u16_at(18);
	let want = match arch {
		"aarch64" => 183,
		_ => 62,
	};
	ensure!(machine == want, "ELF machine {machine} does not match guest arch {arch}");
	let phoff = usize::try_from(u64::from_le_bytes(header[32..40].try_into()?))?;
	let phentsize = usize::from(u16_at(54));
	let phnum = usize::from(u16_at(56));
	for index in 0..phnum {
		let start = phoff + index * phentsize;
		ensure!(start + 4 <= header.len(), "program headers beyond the first 4 KiB");
		let p_type = u32::from_le_bytes(header[start..start + 4].try_into()?);
		ensure!(p_type != 3, "dynamically linked (has PT_INTERP); build the static musl agent");
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn elf(machine: u16, program_types: &[u32]) -> Vec<u8> {
		let mut bytes = vec![0u8; 64 + 56 * program_types.len()];
		bytes[..4].copy_from_slice(b"\x7fELF");
		bytes[4] = 2;
		bytes[5] = 1;
		bytes[18..20].copy_from_slice(&machine.to_le_bytes());
		bytes[32..40].copy_from_slice(&64u64.to_le_bytes());
		bytes[54..56].copy_from_slice(&56u16.to_le_bytes());
		bytes[56..58].copy_from_slice(&u16::try_from(program_types.len()).unwrap().to_le_bytes());
		for (index, p_type) in program_types.iter().enumerate() {
			let start = 64 + index * 56;
			bytes[start..start + 4].copy_from_slice(&p_type.to_le_bytes());
		}
		bytes
	}

	#[test]
	fn static_elf_check_rejects_dynamic_wrong_arch_and_garbage() {
		assert!(check_static_elf(&elf(183, &[6, 1, 1]), "aarch64").is_ok());
		let dynamic = check_static_elf(&elf(183, &[6, 3, 1]), "aarch64").unwrap_err();
		assert!(dynamic.to_string().contains("PT_INTERP"));
		assert!(check_static_elf(&elf(62, &[1]), "aarch64").is_err());
		assert!(check_static_elf(&elf(62, &[1]), "x86_64").is_ok());
		assert!(check_static_elf(b"#!/bin/sh\n", "x86_64").is_err());
	}

	#[test]
	fn kernel_lookup_prefers_explicit_then_bundled_then_dev_tree() {
		let root = tempfile::tempdir().unwrap();
		let bin_dir = root.path().join("target/ws/debug");
		fs::create_dir_all(&bin_dir).unwrap();
		let vmon = bin_dir.join("vmon");
		assert_eq!(local_kernel(None, &vmon, "aarch64").unwrap(), None);

		let dev = root.path().join("target/kernel/Image-aarch64");
		fs::create_dir_all(dev.parent().unwrap()).unwrap();
		fs::write(&dev, b"dev").unwrap();
		assert_eq!(local_kernel(None, &vmon, "aarch64").unwrap(), Some(dev));
		// The dev tree holds one image per arch; x86_64 wants the bzImage.
		assert_eq!(local_kernel(None, &vmon, "x86_64").unwrap(), None);

		let bundled = bin_dir.join("vmon-kernel-aarch64");
		fs::write(&bundled, b"bundled").unwrap();
		assert_eq!(local_kernel(None, &vmon, "aarch64").unwrap(), Some(bundled));

		let explicit = root.path().join("custom-kernel");
		fs::write(&explicit, b"explicit").unwrap();
		assert_eq!(local_kernel(Some(&explicit), &vmon, "aarch64").unwrap(), Some(explicit));
		let missing = root.path().join("missing");
		assert!(local_kernel(Some(&missing), &vmon, "aarch64").is_err());
	}

	#[test]
	fn gpu_driver_lookup_prefers_explicit_then_bundled_then_dev_tree() {
		let root = tempfile::tempdir().unwrap();
		let bin_dir = root.path().join("target/ws/debug");
		fs::create_dir_all(&bin_dir).unwrap();
		let vmon = bin_dir.join("vmon");
		assert_eq!(local_gpu_driver(None, &vmon, "x86_64").unwrap(), None);

		let dev = root.path().join("target/kernel/vmon-nvidia-x86_64");
		fs::create_dir_all(&dev).unwrap();
		assert_eq!(local_gpu_driver(None, &vmon, "x86_64").unwrap(), Some(dev));
		assert_eq!(local_gpu_driver(None, &vmon, "aarch64").unwrap(), None);

		let bundled = bin_dir.join("vmon-nvidia-x86_64");
		fs::create_dir_all(&bundled).unwrap();
		assert_eq!(local_gpu_driver(None, &vmon, "x86_64").unwrap(), Some(bundled.clone()));

		assert_eq!(
			local_gpu_driver(Some(&bundled), &vmon, "aarch64").unwrap(),
			Some(bundled.clone())
		);
		assert!(local_gpu_driver(Some(&bundled.join("missing")), &vmon, "x86_64").is_err());
	}

	#[test]
	fn agent_lookup_names_the_build_command_when_missing() {
		let dir = tempfile::tempdir().unwrap();
		let error = resolve_agent(None, &dir.path().join("vmon")).unwrap_err();
		assert!(error.to_string().contains("just agent-musl"), "{error}");
	}
}
